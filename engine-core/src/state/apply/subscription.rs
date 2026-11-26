//! Subscription-family appliers: message, signal and conditional subscriptions.

use crate::event::Event;
use crate::state::types::*;

pub(super) fn apply_subscription(state: &mut State, event: &Event) {
    match event {
        // Publishing a message is itself a transient fact (a buffered message is
        // recorded by the `MessageBuffered` that follows). The event exists to mint
        // a deterministic message key (carried to the host for its response and
        // restored on replay) and to head the events a correlation produced.
        Event::MessagePublished { .. } => {}

        // Message buffering (#1874): the buffered message, the instances it
        // has correlated to, and its removal on expiry.
        Event::MessageBuffered {
            message_key,
            message_name,
            correlation_key,
            deadline,
            message_id,
            variables,
            correlated_instances,
            ..
        } => {
            let seq = state.next_buffered_seq;
            state.next_buffered_seq = seq.saturating_add(1);
            state.insert_buffered_message(BufferedMessage {
                key: *message_key,
                message_name: message_name.clone(),
                correlation_key: correlation_key.clone(),
                deadline: *deadline,
                message_id: message_id.clone(),
                variables: variables.clone(),
                correlated_instances: correlated_instances.iter().copied().collect(),
                seq,
            });
        }
        Event::BufferedMessageCorrelated {
            message_key,
            instance_key,
            ..
        } => {
            if let Some(message) = state.buffered_messages.get_mut(message_key) {
                message.correlated_instances.insert(*instance_key);
            }
        }
        Event::MessageExpired { message_key, .. } => {
            state.remove_buffered_message(*message_key);
        }

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
                    correlated_message_keys: Default::default(),
                    awaiting_ack: None,
                    rescan_from: None,
                    resume_pending: false,
                    resume_generation: 0,
                    deferred: false,
                    last_answered_delivery: 0,
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
            ..
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
                    correlated_message_keys: Default::default(),
                    awaiting_ack: None,
                    rescan_from: None,
                    resume_pending: false,
                    resume_generation: 0,
                    deferred: false,
                    last_answered_delivery: 0,
                },
            );
        }

        Event::MessageCorrelated {
            subscription_key,
            message_key,
            ..
        } => {
            if let Some(subscription) = state.message_subscriptions.get_mut(subscription_key) {
                // A persistent subscription (non-interrupting boundary or event
                // sub-process) stays open so every matching message spawns
                // another token — recording the message so an at-least-once
                // redelivery of it is recognised; all others settle.
                if subscription.kind.stays_open_after_trigger() {
                    subscription.correlated_message_keys.insert(*message_key);
                } else {
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

        // The routed-delivery ledger record (#1497): fold the source-minted
        // delivery into this partition's dedup / close-tombstone set, bounded
        // by the ledger cap.
        Event::RoutedDeliveryRecorded {
            delivery_key,
            subscription_key,
            close_tombstone,
            ..
        } => {
            record_routed_delivery(
                state,
                RoutedDelivery {
                    delivery_key: *delivery_key,
                    subscription_key: *subscription_key,
                    close_tombstone: *close_tombstone,
                    receipt: None,
                    settled: false,
                },
            );
        }

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
            subscription_key,
            message_key,
            delivery_key,
            ack_partition,
            ..
        } => {
            if ack_partition.is_some() {
                state.message_correlation_ack_used = true;
            }
            if let Some(subscription) = state.message_subscriptions.get_mut(subscription_key) {
                if !subscription.kind.stays_open_after_trigger() {
                    subscription.state = MessageSubscriptionState::Correlated;
                }
                if ack_partition.is_some() {
                    // Ack-paced buffered delivery (#1949): the delivery and its
                    // reservation stay in flight until the instance partition
                    // answers; nothing more is sent meanwhile.
                    subscription.awaiting_ack = Some(AwaitingAck {
                        message_key: *message_key,
                        delivery_key: *delivery_key,
                    });
                }
            }
        }

        // The instance partition answered an ack-paced delivery (#1949). An
        // unconsumed delivery releases the message's reservation (it is
        // available again, to this instance too). A deferral (suspended
        // instance, scope alive) keeps — or, for a one-shot kind, re-opens —
        // the subscription; any other rejection closes it, leaving the untaken
        // buffered messages buffered.
        Event::MessageCorrelationAcknowledged {
            subscription_key,
            message_key,
            accepted,
            consumed,
            deferred,
            element_id,
            ..
        } => {
            let still_buffered = state.buffered_messages.contains_key(message_key);
            if let Some(subscription) = state.message_subscriptions.get_mut(subscription_key) {
                if subscription
                    .awaiting_ack
                    .is_some_and(|a| a.message_key == *message_key)
                {
                    subscription.awaiting_ack = None;
                    // Re-point the canonical subscription at the element the
                    // delivery was actually consumed at (#2037): the instance
                    // may have migrated since the reservation. A legacy ack
                    // carries none and leaves it as is.
                    if let Some(element_id) = element_id {
                        subscription.element_id.clone_from(element_id);
                    }
                    let instance_key = subscription.instance_key;
                    if *deferred {
                        if subscription.state == MessageSubscriptionState::Correlated {
                            subscription.state = MessageSubscriptionState::Open;
                            // Re-opened only to await the resume handshake:
                            // no unpaced live publication may settle it. Not
                            // for the self-routed release of an orphaned
                            // reservation (its message is gone), which is
                            // re-offered the backlog at once.
                            subscription.deferred = still_buffered;
                        }
                    } else if !*accepted && subscription.state == MessageSubscriptionState::Open {
                        subscription.state = MessageSubscriptionState::Correlated;
                    }
                    let index_key = (
                        subscription.message_name.clone(),
                        subscription.correlation_key.clone(),
                    );
                    if *consumed {
                        // Its post-ack scan honours any earlier release (the
                        // engine read `rescan_from` before emitting this).
                        subscription.rescan_from = None;
                    } else if let Some(message) = state.buffered_messages.get_mut(message_key) {
                        message.correlated_instances.remove(&instance_key);
                        // Every OTHER subscription of this instance on the same
                        // (name, correlation key) that is awaiting its own ack
                        // skipped this message while it was reserved: its next
                        // scan must not resume past it.
                        let entry = (message.seq, *message_key);
                        for other in state.message_subscriptions.values_mut() {
                            if other.key != *subscription_key
                                && other.instance_key == instance_key
                                && other.awaiting_ack.is_some()
                                && other.message_name == index_key.0
                                && other.correlation_key == index_key.1
                            {
                                other.rescan_from =
                                    Some(other.rescan_from.map_or(entry, |r| r.min(entry)));
                            }
                        }
                    }
                }
            }
        }

        // The instance partition's answer to an ack-paced delivery (#1949),
        // applied on the EMITTING (instance) partition: record it as that
        // delivery's durable receipt, so a re-driven duplicate is answered
        // with exactly this outcome whatever happens to the subscription.
        Event::MessageCorrelationAcknowledging {
            accepted,
            consumed,
            deferred,
            delivery_key,
            subscription_key,
            message_key,
            instance_key,
            target_partition,
            element_id,
            ..
        } => {
            // Answering a paced delivery is capability use on the RECEIVING
            // side too (Copilot r4702895398): a destination that never applied
            // an enabling envelope must still keep its resume gate open, or the
            // handshake for this deferral is suppressed. Durable (snapshot and
            // replay) and one-way, like the sending side's flag.
            state.message_correlation_ack_used = true;
            // Only the delivery's FIRST answer moves the resume state: a
            // re-emitted duplicate (original receipt, or one recomputed after
            // settlement) must not clear a newer deferral or pending resume.
            let first = *delivery_key == 0
                || state.routed_deliveries.set_receipt(
                    *delivery_key,
                    CorrelationReceipt {
                        accepted: *accepted,
                        consumed: *consumed,
                        deferred: *deferred,
                        message_key: *message_key,
                        instance_key: *instance_key,
                        target_partition: *target_partition,
                        element_id: element_id.clone(),
                    },
                );
            if let Some(s) = state.message_subscriptions.get_mut(subscription_key) {
                // Answer ordering lives with the subscription, not in the
                // bounded ledger: a late duplicate whose ledger entry was
                // evicted re-records as "first", but it answers an OLDER
                // delivery than one already answered, so it must not move
                // the resume state either.
                let newer = *delivery_key == 0 || *delivery_key > s.last_answered_delivery;
                if newer {
                    s.last_answered_delivery = (*delivery_key).max(s.last_answered_delivery);
                }
                if first && newer && s.state == MessageSubscriptionState::Opening {
                    s.resume_pending = *deferred;
                    if *deferred {
                        s.resume_generation = s.resume_generation.wrapping_add(1);
                    }
                }
            }
        }

        // The message partition answered the resume handshake (#1949): the
        // deferred backlog is re-offered from here, so the record may take
        // live publications again.
        Event::MessageSubscriptionResumed {
            subscription_key, ..
        } => {
            if let Some(s) = state.message_subscriptions.get_mut(subscription_key) {
                s.deferred = false;
            }
        }

        // The message partition confirmed the resume handshake (#1949).
        // Only the CURRENT generation clears it: a delayed confirmation of an
        // older resume must not erase a newer obligation.
        Event::MessageSubscriptionResumeConfirmed {
            subscription_key,
            generation,
            ..
        } => {
            if let Some(s) = state.message_subscriptions.get_mut(subscription_key) {
                if s.resume_generation == *generation {
                    s.resume_pending = false;
                }
            }
        }
        // The message partition processed this delivery's ack (#1949):
        // unpin its receipt.
        Event::MessageCorrelationReceiptSettled { delivery_key, .. } => {
            state.routed_deliveries.settle_receipt(*delivery_key);
        }
        Event::RemoteBufferedDeliveryActivated { .. } => {
            state.remote_buffered_delivery_latched = true;
        }

        Event::MessageStartSubscriptionCreated {
            process_definition_key,
            process_id,
            message_name,
            start_element_id,
            tool_name,
            tool_properties,
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
                    tool_name: tool_name.clone(),
                    tool_properties: tool_properties.clone(),
                },
            );
        }
        _ => unreachable!(),
    }
}
