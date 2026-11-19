use super::*;
use crate::agent::{
    AgentHistoryCommitStatus, AgentHistoryRecord, AgentHistoryRole, AgentHistoryTurn,
    AgentInstance, AgentInstanceStatus,
};

impl Engine {
    pub(super) fn agent_type_for_element(
        &self,
        instance_key: Key,
        element_id: &str,
        element_instance_key: Key,
    ) -> Result<crate::agent::AgentType, EngineError> {
        match self.element_kind(instance_key, element_id) {
            Some(crate::model::ElementKind::ServiceTask {
                agent_type: Some(agent_type),
                ..
            }) => Ok(agent_type),
            Some(crate::model::ElementKind::ServiceTask { .. }) => {
                Err(EngineError::AgentInstanceMissingAgentDefinition {
                    element_instance_key,
                })
            }
            _ => Err(EngineError::AgentInstanceElementNotEligible {
                element_instance_key,
            }),
        }
    }

    pub(super) fn validate_agent_history(
        history: &[AgentHistoryTurn],
        creating: bool,
    ) -> Result<(), EngineError> {
        let invalid = |reason: &str| EngineError::AgentHistoryInvalid {
            reason: reason.to_string(),
        };
        let mut ids = BTreeSet::new();
        for turn in history {
            let id = turn
                .history_item_id
                .as_deref()
                .filter(|id| !id.trim().is_empty())
                .ok_or_else(|| invalid("historyItemId is required"))?;
            if !ids.insert(id) {
                return Err(invalid("historyItemId must be unique within the request"));
            }
            if turn.loop_iteration < 1 {
                return Err(invalid("loopIteration must be positive"));
            }
            if turn.role == AgentHistoryRole::Configuration {
                let attributes = Self::configuration_attributes(turn);
                if attributes.is_empty()
                    || attributes.iter().any(|name| {
                        !matches!(
                            name.as_str(),
                            "model"
                                | "provider"
                                | "systemPrompt"
                                | "tools"
                                | "maxTokens"
                                | "maxModelCalls"
                                | "maxToolCalls"
                        )
                    })
                {
                    return Err(invalid(
                        "CONFIGURATION must change supported configuration fields",
                    ));
                }
            }
        }
        if creating && !history.is_empty() {
            for field in ["model", "provider", "systemPrompt"] {
                if !history.iter().any(|turn| {
                    turn.role == AgentHistoryRole::Configuration
                        && match field {
                            "model" => turn
                                .model
                                .as_deref()
                                .is_some_and(|value| !value.trim().is_empty()),
                            "provider" => turn
                                .provider
                                .as_deref()
                                .is_some_and(|value| !value.trim().is_empty()),
                            _ => turn
                                .system_prompt
                                .as_ref()
                                .is_some_and(|blocks| !blocks.is_empty()),
                        }
                }) {
                    return Err(invalid(
                        "CREATE history must establish model, provider and systemPrompt",
                    ));
                }
            }
        }
        Ok(())
    }

    pub(super) fn configuration_attributes(turn: &AgentHistoryTurn) -> Vec<String> {
        if !turn.changed_attributes.is_empty() {
            return turn.changed_attributes.clone();
        }
        let mut attributes = Vec::new();
        for (present, field) in [
            (turn.model.is_some(), "model"),
            (turn.provider.is_some(), "provider"),
            (turn.system_prompt.is_some(), "systemPrompt"),
            (!turn.tools.is_empty(), "tools"),
            (turn.limits.is_some(), "maxTokens"),
            (turn.limits.is_some(), "maxModelCalls"),
            (turn.limits.is_some(), "maxToolCalls"),
        ] {
            if present {
                attributes.push(field.to_string());
            }
        }
        attributes
    }

    fn apply_agent_configuration(agent: &mut AgentInstance, turn: &AgentHistoryTurn) {
        if turn.role != AgentHistoryRole::Configuration {
            return;
        }
        for field in Self::configuration_attributes(turn) {
            match field.as_str() {
                "model" => agent.definition.model = turn.model.clone(),
                "provider" => agent.definition.provider = turn.provider.clone(),
                "systemPrompt" => agent.definition.system_prompt = turn.system_prompt.clone(),
                "tools" => agent.tools = turn.tools.clone(),
                "maxTokens" => agent.limits.max_tokens = turn.limits.unwrap_or_default().max_tokens,
                "maxModelCalls" => {
                    agent.limits.max_model_calls = turn.limits.unwrap_or_default().max_model_calls
                }
                "maxToolCalls" => {
                    agent.limits.max_tool_calls = turn.limits.unwrap_or_default().max_tool_calls
                }
                _ => {}
            }
        }
    }

    pub(super) fn duplicate_agent_history(
        &self,
        agent: Key,
        turn: &AgentHistoryTurn,
    ) -> Option<Key> {
        let id = turn
            .history_item_id
            .as_deref()
            .filter(|id| !id.is_empty())?;
        let instance = self.find_agent_instance(agent)?;
        self.state
            .instances
            .get(&instance.process_instance_key)?
            .agent_history
            .get(&agent)?
            .iter()
            .find(|record| {
                record.history_item_id.as_deref() == Some(id)
                    && (record.commit_status == AgentHistoryCommitStatus::Committed
                        || (record.commit_status == AgentHistoryCommitStatus::Pending
                            && record.job_key == turn.job_key
                            && record.job_lease == turn.job_lease))
            })
            .map(|record| record.agent_history_key)
    }

    pub(super) fn apply_history_changes(
        &self,
        agent: &mut AgentInstance,
        history: &[AgentHistoryTurn],
        creating: bool,
    ) {
        for turn in history {
            if self
                .duplicate_agent_history(agent.agent_instance_key, turn)
                .is_some()
            {
                continue;
            }
            if creating {
                Self::apply_agent_configuration(agent, turn);
            }
            let metrics = turn.metrics.unwrap_or_default();
            let delta = crate::agent::AgentInstanceMetricsDelta {
                input_tokens: metrics.input_tokens.unwrap_or_default(),
                output_tokens: metrics.output_tokens.unwrap_or_default(),
                reasoning_token_count: metrics.reasoning_token_count.unwrap_or_default(),
                cache_creation_token_count: metrics.cache_creation_token_count.unwrap_or_default(),
                cache_read_token_count: metrics.cache_read_token_count.unwrap_or_default(),
                model_calls: i64::from(turn.role == AgentHistoryRole::Assistant),
                tool_calls: if turn.role == AgentHistoryRole::Assistant {
                    turn.tool_calls.len() as i64
                } else {
                    0
                },
            };
            agent.metrics = agent.metrics.with_delta(&delta);
        }
    }

    pub(super) fn settle_job_history(
        &mut self,
        log: &mut Vec<Event>,
        instance_key: Key,
        job_key: Key,
        lease: &str,
        commit: bool,
    ) {
        let mut records: Vec<AgentHistoryRecord> = self
            .state
            .instances
            .get(&instance_key)
            .into_iter()
            .flat_map(|instance| instance.agent_history.values())
            .flatten()
            .filter(|record| {
                record.job_key == job_key
                    && record.commit_status == AgentHistoryCommitStatus::Pending
            })
            .cloned()
            .collect();
        records.sort_by_key(|record| record.agent_history_key);
        for record in records {
            let winning = commit && (lease.is_empty() || record.job_lease == lease);
            let agent_instance_key = record.agent_instance_key;
            let keys = vec![record.agent_history_key];
            self.emit(
                log,
                if winning {
                    Event::AgentHistoryCommitted {
                        instance_key,
                        agent_instance_key,
                        agent_history_keys: keys,
                    }
                } else {
                    Event::AgentHistoryDiscarded {
                        instance_key,
                        agent_instance_key,
                        agent_history_keys: keys,
                    }
                },
            );
            if winning && record.role == AgentHistoryRole::Configuration {
                if let Some(mut agent) = self.find_agent_instance(agent_instance_key).cloned() {
                    Self::apply_agent_configuration(
                        &mut agent,
                        &AgentHistoryTurn {
                            role: record.role,
                            changed_attributes: record.changed_attributes,
                            model: record.model,
                            provider: record.provider,
                            system_prompt: record.system_prompt,
                            tools: record.tools,
                            limits: record.limits,
                            ..Default::default()
                        },
                    );
                    self.emit(
                        log,
                        Event::AgentInstanceUpdated {
                            instance_key,
                            agent_instance: agent,
                        },
                    );
                }
            }
        }
    }

    pub(super) fn cleanup_agent_instances(&mut self, log: &mut Vec<Event>, instance_key: Key) {
        let mut agents: Vec<_> = self
            .state
            .instances
            .get(&instance_key)
            .into_iter()
            .flat_map(|instance| instance.agent_instances.values())
            .filter(|agent| agent.status.is_active())
            .cloned()
            .collect();
        agents.sort_by_key(|agent| agent.agent_instance_key);
        for mut agent in agents {
            log.extend(self.discard_agent_history(agent.agent_instance_key));
            agent.status = AgentInstanceStatus::Completed;
            agent.completed_at = self.now;
            agent.last_updated_at = self.now;
            self.emit(
                log,
                Event::AgentInstanceCompleted {
                    instance_key,
                    agent_instance: agent,
                },
            );
        }
    }
}
