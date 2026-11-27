# ADR 0067 — Event-based gateway runtime latch and replay contract

Status: **Accepted**, 2026-11-27.
Relates to: ADR 0066 (`0066-replication-model.md`), issues #1676 (legacy race), #1963 / #2003
(gateway-owned subscriptions), #2004 (this runtime), PR #2047, #1071 (migration-by-replay).

## Decision

The event-based gateway owns its catch subscriptions (Zeebe `EventTrigger` semantics):

- **Arbitration is per batch: the first trigger in log order wins.** When several catches of one
  waiting gateway become ready together, the lowest-ordered one is recorded as the trigger
  (`Event::EventGatewayTriggered`) by the pure `event_gateway_trigger` kernel.
- **The winning payload is snapshotted at trigger time** (`EventGatewayTriggered::variables`,
  `GatewayTrigger::variables`) and **written LOCAL to the catch element instance when it
  activates**. The catch's listeners, output mappings and propagation read it from there.
- **Losers are cancelled with no trigger record**: their subscriptions are deleted with the
  gateway's; only the winner is recorded.
- **Late messages** that arrive after the trigger are not correlated to the gateway: they stay
  buffered (subject to their TTL) or are released by the acknowledgement, as for any message
  without a waiting subscription.

## Replay contract (the `event-gateway-runtime-v1` latch)

The new runtime is enabled by a one-way, journaled latch, `Event::EventGatewayRuntimeActivated`
(`Engine::event_gateway_runtime_latched`):

- **Before the latch**, journal entries replay with the **legacy** (#1676) semantics: the gateway
  passes through and arms its catches as a race.
- **After the latch**, entries replay with the **new-runtime** semantics above. The engine records
  the latch only on a new-runtime activation, when the replicated envelope enables the capability
  and no legacy event-gateway state is left, so a latched log never holds any.
- **Legacy event-gateway state on a latched partition is refused fail-closed**: legacy state is any
  of the three kinds the latch decision waits on (one shared predicate,
  `State::is_legacy_event_gateway_blocker`): an armed legacy race, a race-less pre-#1676 gateway
  catch, or a legacy gateway still in flight. Loading a latched snapshot or journal that holds any
  of them fails with the typed `LegacyEventGatewayRace` error naming the
  instances (settle or cancel them on the previous release first). It is never silently dropped.
- **Cluster feature gate**: the capability is enabled only when every cluster member advertises
  `FEATURE_EVENT_GATEWAY_RUNTIME` (`event-gateway-runtime-v1`, `nano_falcon_protocol::cluster_join`);
  a single node is trivially on. A mixed-version cluster stays on the legacy runtime. Once any
  partition is latched, `required_cluster_features` permanently requires
  `FEATURE_EVENT_GATEWAY_RUNTIME` of joining peers, so no peer without the capability can join.
