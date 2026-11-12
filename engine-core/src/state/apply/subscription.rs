//! Subscription-family appliers: message, signal and conditional subscriptions.

use crate::event::Event;
use crate::state::types::*;

pub(super) fn apply_subscription(state: &mut State, event: &Event) {
    match event {
        // Publishing a message is, in nano, a transient fact: messages are not
        // buffered, so there is nothing to record. The event exists only to mint
        // a deterministic message key (carried to the host for its response and
        // restored on replay) and to head the events a correlation produced.
        Event::MessagePublished { .. } => {}

        Event::MessageSubscriptionCreated {
            subscription_key,
            instance_key,
            element_instance_key,
            element_id,
            message_name,
            correlation_key,
            kind,
        } => {
            state.message_subscriptions.insert(
                *subscription_key,
                MessageSubscription {
                    key: *subscription_key,
                    instance_key: *instance_key,
                    element_instance_key: *element_instance_key,
                    element_id: element_id.clone(),
                    message_name: message_name.clone(),
                    correlation_key: correlation_key.clone(),
                    state: MessageSubscriptionState::Open,
                    kind: kind.clone(),
                },
            );
        }

        // The instance partition's pending view of a subscription whose canonical
        // record lives on the message partition (`hash(correlation_key)`). Holds
        // the token until a `CorrelateMessageSubscription` continuation arrives.
        Event::MessageSubscriptionOpening {
            subscription_key,
            instance_key,
            element_instance_key,
            element_id,
            message_name,
            correlation_key,
            kind,
        } => {
            state.message_subscriptions.insert(
                *subscription_key,
                MessageSubscription {
                    key: *subscription_key,
                    instance_key: *instance_key,
                    element_instance_key: *element_instance_key,
                    element_id: element_id.clone(),
                    message_name: message_name.clone(),
                    correlation_key: correlation_key.clone(),
                    state: MessageSubscriptionState::Opening,
                    kind: kind.clone(),
                },
            );
        }

        Event::MessageCorrelated {
            subscription_key, ..
        } => {
            if let Some(subscription) = state.message_subscriptions.get_mut(subscription_key) {
                // A non-interrupting boundary subscription stays open so every
                // matching message spawns another token; all others settle.
                if !subscription.kind.stays_open_after_trigger() {
                    subscription.state = MessageSubscriptionState::Correlated;
                }
            }
        }

        Event::MessageSubscriptionCanceled {
            subscription_key, ..
        } => {
            if let Some(subscription) = state.message_subscriptions.get_mut(subscription_key) {
                subscription.state = MessageSubscriptionState::Canceled;
            }
        }

        // A signal subscription was opened on a signal intermediate catch event
        // (the token rests on it) or as a signal boundary on an activity.
        Event::SignalSubscriptionCreated {
            subscription_key,
            instance_key,
            element_instance_key,
            element_id,
            signal_name,
            kind,
        } => {
            state.signal_subscriptions.insert(
                *subscription_key,
                SignalSubscription {
                    key: *subscription_key,
                    instance_key: *instance_key,
                    element_instance_key: *element_instance_key,
                    element_id: element_id.clone(),
                    signal_name: signal_name.clone(),
                    state: MessageSubscriptionState::Open,
                    kind: kind.clone(),
                },
            );
        }

        // A broadcast signal correlated to an open subscription. Settles it
        // (unless it is a non-interrupting boundary, which stays open so every
        // broadcast spawns another token), exactly like `MessageCorrelated`.
        Event::SignalCorrelated {
            subscription_key, ..
        } => {
            if let Some(subscription) = state.signal_subscriptions.get_mut(subscription_key) {
                if !subscription.kind.stays_open_after_trigger() {
                    subscription.state = MessageSubscriptionState::Correlated;
                }
            }
        }

        // An open signal subscription was cancelled because the element it
        // guarded left the flow first (mirrors `MessageSubscriptionCanceled`).
        Event::SignalSubscriptionCanceled {
            subscription_key, ..
        } => {
            if let Some(subscription) = state.signal_subscriptions.get_mut(subscription_key) {
                subscription.state = MessageSubscriptionState::Canceled;
            }
        }

        Event::ConditionalSubscriptionCreated {
            subscription_key,
            instance_key,
            element_instance_key,
            element_id,
            condition,
            referenced_vars,
            kind,
        } => {
            state.conditional_subscriptions.insert(
                *subscription_key,
                ConditionalSubscription {
                    key: *subscription_key,
                    instance_key: *instance_key,
                    element_instance_key: *element_instance_key,
                    element_id: element_id.clone(),
                    condition: condition.clone(),
                    referenced_vars: referenced_vars.clone(),
                    state: MessageSubscriptionState::Open,
                    kind: kind.clone(),
                },
            );
        }

        // A conditional subscription's condition became true. Settles it (unless
        // it is a non-interrupting boundary, which stays open so every satisfying
        // variable change spawns another token), exactly like `SignalCorrelated`.
        Event::ConditionalTriggered {
            subscription_key, ..
        } => {
            if let Some(subscription) = state.conditional_subscriptions.get_mut(subscription_key) {
                if !subscription.kind.stays_open_after_trigger() {
                    subscription.state = MessageSubscriptionState::Correlated;
                }
            }
        }

        // An open conditional subscription was cancelled because the element it
        // guarded left the flow first (mirrors `SignalSubscriptionCanceled`).
        Event::ConditionalSubscriptionCanceled {
            subscription_key, ..
        } => {
            if let Some(subscription) = state.conditional_subscriptions.get_mut(subscription_key) {
                subscription.state = MessageSubscriptionState::Canceled;
            }
        }

        // A signal was broadcast: records no durable state (signals are not
        // buffered); carries the minted `signal_key` to restore the key
        // generator on replay, exactly like `MessagePublished`.
        Event::SignalBroadcast { .. } => {}

        // The instance partition tearing down a cross-partition parked
        // placeholder: mark it cancelled locally exactly like
        // `MessageSubscriptionCanceled`. The host routes a
        // `CloseMessageSubscription` to disarm the canonical record on the
        // message partition.
        Event::MessageSubscriptionClosing {
            subscription_key, ..
        } => {
            if let Some(subscription) = state.message_subscriptions.get_mut(subscription_key) {
                subscription.state = MessageSubscriptionState::Canceled;
            }
        }

        // A match found on the message partition for a subscription whose instance
        // lives on another partition: settle the canonical record exactly as
        // `MessageCorrelated` does (a non-interrupting boundary stays open). The
        // token advance happens on the instance partition, driven by the
        // `CorrelateMessageSubscription` continuation the host routes there.
        Event::RemoteMessageCorrelation {
            subscription_key, ..
        } => {
            if let Some(subscription) = state.message_subscriptions.get_mut(subscription_key) {
                if !subscription.kind.stays_open_after_trigger() {
                    subscription.state = MessageSubscriptionState::Correlated;
                }
            }
        }

        Event::MessageStartSubscriptionCreated {
            process_definition_key,
            process_id,
            message_name,
            start_element_id,
        } => {
            // Keyed by message name: re-deploying a process with the same start
            // message replaces the older version's subscription.
            state.message_start_subscriptions.insert(
                message_name.clone(),
                MessageStartSubscription {
                    process_definition_key: *process_definition_key,
                    process_id: process_id.clone(),
                    message_name: message_name.clone(),
                    start_element_id: start_element_id.clone(),
                },
            );
        }
        _ => unreachable!(),
    }
}
