# TEST-MATRIX.md — resilient-pubsub-e2e-nestjs

## Suite × Coverage matrix

| Suite | File | Library features exercised | Nest-specific proof |
|---|---|---|---|
| Publish → subscribe | `test/publish-subscribe.e2e.test.ts` | `createResilientPublisher`, `createResilientSubscriber`, `publish()`, `on()`, `start()`, `stop()` | Publisher injected via `PUBSUB_CLIENT`; subscriber wired through `SubscriberService` + `OnModuleInit` |
| Context propagation | `test/context-propagation.e2e.test.ts` | `propagation.allowlist`, W3C `traceparent` auto-propagation, header extraction | Same DI wiring; propagation options passed through `SubscriberService.configure()` |
| Dead-letter | `test/dead-letter.e2e.test.ts` | `buildDeadLetterPolicy`, `getDeliveryAttempt`, nack → redeliver cycle, DLQ poll with `returnImmediately: true` | Handler-always-throws scenario via `SubscriberService`; DLQ pull uses low-level `v1.SubscriberClient` |
| Graceful shutdown | `test/graceful-shutdown.e2e.test.ts` | `stop()` graceful drain, `stopTimeoutMs`, in-flight handler completion | `OnApplicationShutdown` → `stop()` triggered by `module.close()`; proves consumer needs zero manual wiring |

## Framework-agnosticism note

The four suites above mirror the same scenarios covered by the sibling plain-Node e2e repository (`resilient-pubsub-e2e`). Running both repositories against the same library SHA demonstrates that:

- The library API is identical in both environments.
- No Nest-specific shims or adapters are required in the library itself.
- The only difference is the consumption model: Nest DI + lifecycle hooks vs. direct function calls.

The `SubscriberService` pattern shown here (`OnModuleInit` → `start()`, `OnApplicationShutdown` → `stop()`) is the recommended idiomatic integration for any NestJS application that uses `resilient-pubsub`.
