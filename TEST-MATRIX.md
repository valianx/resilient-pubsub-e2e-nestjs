# TEST-MATRIX.md — resilient-pubsub-e2e-nestjs

## Suite × Coverage matrix

| Suite | File | Library features exercised | Nest-specific proof |
|---|---|---|---|
| Publish → subscribe | `test/publish-subscribe.e2e.test.ts` | `createResilientPublisher`, `createResilientSubscriber`, `publish()`, `on()`, `start()`, `stop()` | Publisher injected via `PUBSUB_CLIENT`; subscriber wired through `SubscriberService` + `OnModuleInit` |
| Context propagation | `test/context-propagation.e2e.test.ts` | `propagation.allowlist`, W3C `traceparent` auto-propagation, header extraction | Same DI wiring; propagation options passed through `SubscriberService.configure()` |
| Dead-letter | `test/dead-letter.e2e.test.ts` | `buildDeadLetterPolicy`, `getDeliveryAttempt`, nack → redeliver cycle, DLQ poll with `returnImmediately: true` | Handler-always-throws scenario via `SubscriberService`; DLQ pull uses low-level `v1.SubscriberClient` |
| Graceful shutdown | `test/graceful-shutdown.e2e.test.ts` | `stop()` graceful drain, `stopTimeoutMs`, in-flight handler completion | `OnApplicationShutdown` → `stop()` triggered by `module.close()`; proves consumer needs zero manual wiring |
| Ordering keys | `test/ordering.e2e.test.ts` | `ordering: true` publisher, `enableMessageOrdering` subscription, same-orderingKey in-order delivery | Subscriber wired through `SubscriberService`; ordering options pass through the Nest DI layer |
| Custom serializer | `test/serializer-custom.e2e.test.ts` | Custom `Serializer<T>`, `SerializationError`, `content-type` attribute on the wire, raw `v1.SubscriberClient` pull | `serializer` option injected via `SubscriberService.configure()`; proves serializer flows through DI correctly |
| Poison message | `test/poison.e2e.test.ts` | `onPoison` hook, raw non-JSON bytes via native `topic.publishMessage()`, deserialization failure path | `hooks.onPoison` passed through `SubscriberService.configure()`; handler NOT invoked; `onNack` NOT fired |
| Observability hooks | `test/hooks.e2e.test.ts` | `onPublish`, `onAck`, `onNack`, `onError`, `isResilientPubSubError` | Subscriber hooks injected via `SubscriberService.configure()`; each scenario uses an isolated `TestingModule` |
| Redelivery | `test/redelivery.e2e.test.ts` | At-least-once guarantee, nack → redeliver → success, delivery counter | Handler-throws-on-first-delivery pattern wired through `SubscriberService`; eventual ack proven |
| Flow control | `test/flow-control.e2e.test.ts` | `flowControl.maxMessages` (bounds outstanding messages, not handler concurrency), all-N-processed-once | `flowControl` option passed via `SubscriberService.configure()`; peak concurrency logged only (not asserted) |
| Env-var config | `test/env-config.e2e.test.ts` | `resolveConfigFromEnv`, all `RESILIENT_PUBSUB_*` variables, lenient-parse policy | No Nest DI needed; unit-style assertion against the real installed library build |

## Key learnings replicated from the Node repo

- **flow-control**: `maxMessages` bounds OUTSTANDING (unacked) messages — i.e. prefetch — NOT handler concurrency. Assert all N messages processed exactly once; do NOT assert peak concurrency `<= maxMessages` (log it only).
- **hooks / onRetry-live**: Not tested because the emulator classifies failures as permanent (NOT_FOUND / UNAVAILABLE → non-retriable). Forcing a transient failure deterministically is unreliable. The retry path is covered by unit tests in the library's own suite.
- **poison**: Publish raw non-JSON bytes via the native `topic.publishMessage()` API to control the wire bytes exactly. The `JsonSerializer` subscriber cannot decode them → handler NOT invoked, `onPoison` fires, message is nacked (not `onNack` — only `onPoison` fires on the deserialization-failure path).
- **dead-letter DLQ poll**: Use `returnImmediately: true` in `v1.SubscriberClient.pull()` for best-effort non-blocking DLQ inspection.
- **ordering**: Create the subscription with `enableMessageOrdering: true` and the publisher with `ordering: true`; share the same `orderingKey` for all messages to guarantee in-order delivery.

## Framework-agnosticism note

The eleven suites above mirror the scenarios covered by the sibling plain-Node e2e repository (`resilient-pubsub-e2e`). Running both repositories against the same library SHA demonstrates that:

- The library API is identical in both environments.
- No Nest-specific shims or adapters are required in the library itself.
- The only difference is the consumption model: Nest DI + lifecycle hooks vs. direct function calls.

The `SubscriberService` pattern shown here (`OnModuleInit` → `start()`, `OnApplicationShutdown` → `stop()`) is the recommended idiomatic integration for any NestJS application that uses `resilient-pubsub`.
