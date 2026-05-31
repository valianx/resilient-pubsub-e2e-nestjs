# resilient-pubsub-e2e-nestjs

End-to-end consumer test repository for the [`resilient-pubsub`](https://github.com/valianx/resilient-pubsub) library, consumed from a **NestJS application** and exercised against the **Google Cloud Pub/Sub emulator**.

## Purpose

This repository proves two things:

1. **Framework-agnosticism** — the `resilient-pubsub` library works identically inside the NestJS dependency-injection container as it does in plain Node. Same API, same semantics, same test scenarios.

2. **NestJS lifecycle integration** — the library's graceful-shutdown feature (`subscriber.stop()`) integrates cleanly with Nest's `OnModuleInit` and `OnApplicationShutdown` hooks, so consumer code gets drain-before-exit for free without manual `SIGTERM` wiring.

The library is consumed as a **git dependency pinned to a specific commit SHA** that ships a prebuilt `dist/`:

```
"resilient-pubsub": "github:valianx/resilient-pubsub#de036badb4cd5e40c09cf517ce86abc74d85c24e"
```

No Redis — v0.1 has no dedup store.

## NestJS integration design

| File | Role |
|---|---|
| `src/pubsub.module.ts` | NestJS module; provides the shared `PubSub` client under `PUBSUB_CLIENT` injection token; imports `SubscriberService` |
| `src/publisher.provider.ts` | `makePublisherProvider<T>()` — factory that returns a `FactoryProvider` wrapping `createResilientPublisher` with the shared client injected |
| `src/subscriber.service.ts` | Injectable service wrapping `createResilientSubscriber`; `OnModuleInit` → `start()`; `OnApplicationShutdown` → `stop()` (graceful drain) |

### Lifecycle hooks

```
module.init()
  └─ OnModuleInit → SubscriberService.onModuleInit()
       └─ subscriber.start()   ← begins consuming messages

module.close()   (or SIGTERM in production)
  └─ OnApplicationShutdown → SubscriberService.onApplicationShutdown()
       └─ subscriber.stop()   ← drains in-flight handlers, then closes
```

## Test suites

| Suite | File | What it proves |
|---|---|---|
| Publish → subscribe | `test/publish-subscribe.e2e.test.ts` | Round-trip through Nest DI: publish → handler receives typed body → ack |
| Context propagation | `test/context-propagation.e2e.test.ts` | Allowlist gate: `traceparent` + `x-tenant-id` propagate, `x-secret` is dropped |
| Dead-letter | `test/dead-letter.e2e.test.ts` | `getDeliveryAttempt(meta)` increases across nack redeliveries; optional DLQ poll |
| Graceful shutdown | `test/graceful-shutdown.e2e.test.ts` | `app.close()` waits for the in-flight handler to complete via `OnApplicationShutdown` |

## Running locally

You need Docker running locally to start the emulator.

```bash
# Start the Pub/Sub emulator
docker run -d --name pubsub-emulator -p 8085:8085 \
  gcr.io/google.com/cloudsdktool/google-cloud-cli:emulators \
  gcloud beta emulators pubsub start \
    --host-port=0.0.0.0:8085 \
    --project=e2e-project

# Wait for the port (macOS/Linux)
until nc -z localhost 8085; do sleep 1; done && sleep 5

# Install dependencies
pnpm install --no-frozen-lockfile

# Type-check
pnpm typecheck

# Run all e2e suites
PUBSUB_EMULATOR_HOST=localhost:8085 PROJECT_ID=e2e-project GOOGLE_CLOUD_PROJECT=e2e-project pnpm test
```

## CI

The GitHub Actions workflow (`.github/workflows/e2e.yml`) mirrors the proven setup from the sibling plain-Node e2e repo:

- Official `gcr.io/google.com/cloudsdktool/google-cloud-cli:emulators` image started with `docker run -d` (not `services:` — the image's default command is a shell, not the emulator)
- Port 8085, wait-for-port loop with 5 s grace sleep
- SHA-pinned actions (identical SHAs to the Node repo, already verified real)
- `pnpm install --no-frozen-lockfile` (git dep SHA means lockfile is always stale)
- `pnpm typecheck` then `pnpm test`
- Emulator container logs on failure
- `timeout-minutes: 15`

## Requirements

- Node.js 24+
- pnpm 11+
- Docker (for local emulator)
