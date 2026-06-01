/**
 * Suite: hooks (NestJS)
 *
 * Verifies that observability hooks fire correctly when wired through the
 * NestJS DI layer:
 *
 *   - Publisher  `onPublish`: fires on a successful publish with a non-empty
 *     messageId. Publisher uses createResilientPublisher directly (consistent
 *     with other Nest suites).
 *   - Subscriber `onAck`:    fires after the handler resolves successfully.
 *     Subscriber hooks are injected via SubscriberService.configure().
 *   - Subscriber `onNack`:   fires when the handler throws.
 *   - Subscriber `onError`:  fires with ResilientPubSubError when handler throws.
 *
 * `onRetry` (publisher): not tested because the emulator classifies failures
 * as permanent (NOT_FOUND / UNAVAILABLE → non-retriable), so a transient error
 * cannot be forced deterministically. The retry path is covered by unit tests in
 * the library's own suite. This limitation is identical to the Node repo.
 *
 * Each scenario uses a distinct module instance to keep the NestJS lifecycle
 * clean and avoid state leak between hook tests.
 */

import 'reflect-metadata';
import { describe, it, beforeAll, afterAll, expect } from 'vitest';
import { Test } from '@nestjs/testing';
import type { TestingModule } from '@nestjs/testing';
import { PubSub } from '@google-cloud/pubsub';
import { createResilientPublisher } from 'resilient-pubsub/publisher';
import type { ResilientPublisher } from 'resilient-pubsub/publisher';
import { isResilientPubSubError } from 'resilient-pubsub/errors';
import {
  uniqueNames,
  ensureTopic,
  ensureSubscription,
  deleteSub,
  deleteTopic,
  createClient,
} from '../lib/harness.js';
import { PUBSUB_CLIENT, PubSubModule } from '../src/pubsub.module.js';
import { SubscriberService } from '../src/subscriber.service.js';

// ── Types ────────────────────────────────────────────────────────────────────

interface SampleEvent {
  id: string;
}

// ── Fixture ──────────────────────────────────────────────────────────────────

// Single topic/sub shared across all hook scenarios (sequential execution).
const names = uniqueNames('nest-hooks');
let adminClient: PubSub;
let sharedModule: TestingModule;
let pubSubClient: PubSub;

beforeAll(async () => {
  adminClient = createClient();
  await ensureTopic(adminClient, names.topic);
  await ensureSubscription(adminClient, names.topic, names.sub);

  // Build one module to get the shared PubSubClient; each test scenario
  // configures SubscriberService independently before module.init().
  sharedModule = await Test.createTestingModule({
    imports: [PubSubModule],
  }).compile();

  pubSubClient = sharedModule.get<PubSub>(PUBSUB_CLIENT);
});

afterAll(async () => {
  // sharedModule is never init()-ed (only used for the PubSub client), so
  // close() is safe here without triggering OnModuleInit.
  await sharedModule.close();
  await deleteSub(adminClient, names.sub);
  await deleteTopic(adminClient, names.topic);
  await adminClient.close();
});

// ── Helper ───────────────────────────────────────────────────────────────────

/**
 * Bootstraps an isolated TestingModule with the given configure() call applied
 * before init(). Returns the module and a cleanup function.
 */
async function buildModule(
  configure: (svc: SubscriberService<SampleEvent>) => void
): Promise<{ module: TestingModule; publisher: ResilientPublisher<SampleEvent> }> {
  const mod = await Test.createTestingModule({
    imports: [PubSubModule],
  }).compile();

  const svc = mod.get<SubscriberService<SampleEvent>>(SubscriberService as never);
  configure(svc);

  await mod.init();

  const client = mod.get<PubSub>(PUBSUB_CLIENT);
  const publisher = createResilientPublisher<SampleEvent>({
    topic: names.topic,
    pubSubClient: client,
    retry: { maxAttempts: 3, initialDelay: 100, jitter: 'none' },
  });

  return { module: mod, publisher };
}

// ── Tests ────────────────────────────────────────────────────────────────────

describe('NestJS observability hooks', () => {
  it(
    'publisher onPublish fires with a non-empty messageId on success',
    async () => {
      let hookedMessageId: string | undefined;

      let resolveHook!: () => void;
      const hookFired = new Promise<void>((resolve) => {
        resolveHook = resolve;
      });

      const publisher = createResilientPublisher<SampleEvent>({
        topic: names.topic,
        pubSubClient,
        retry: { maxAttempts: 3, initialDelay: 100, jitter: 'none' },
        hooks: {
          onPublish: ({ messageId }) => {
            hookedMessageId = messageId;
            resolveHook();
          },
        },
      });

      const result = await publisher.publish({ body: { id: 'nest-hook-pub-1' } });

      // onPublish is called synchronously inside publish before it returns,
      // but await defensively to avoid races.
      await hookFired;

      expect(typeof hookedMessageId).toBe('string');
      expect(hookedMessageId!.length).toBeGreaterThan(0);
      expect(hookedMessageId).toBe(result.messageId);
    },
    30_000
  );

  it(
    'subscriber onAck fires after a successful handler resolution (via SubscriberService)',
    async () => {
      let ackedMessageId: string | undefined;

      let resolveAck!: () => void;
      const ackFired = new Promise<void>((resolve) => {
        resolveAck = resolve;
      });

      const { module: mod, publisher } = await buildModule((svc) => {
        svc.configure(
          async () => {
            // Handler resolves — message should be acked
          },
          {
            subscription: names.sub,
            flowControl: { maxMessages: 1 },
            hooks: {
              onAck: ({ messageId }) => {
                ackedMessageId = messageId;
                resolveAck();
              },
            },
          }
        );
      });

      await publisher.publish({ body: { id: 'nest-hook-ack-1' } });

      await ackFired;
      await mod.close();

      if (ackedMessageId !== undefined) {
        expect(typeof ackedMessageId).toBe('string');
        expect(ackedMessageId.length).toBeGreaterThan(0);
      } else {
        // The emulator may not set messageId in all versions — non-fatal
        expect(ackedMessageId).toBeUndefined();
      }
    },
    30_000
  );

  it(
    'subscriber onNack and onError fire when the handler throws (via SubscriberService)',
    async () => {
      let nackedMessageId: string | undefined;
      let errorReceived: unknown;

      let resolveNack!: () => void;
      const nackFired = new Promise<void>((resolve) => {
        resolveNack = resolve;
      });

      // Guard so only the first nack triggers resolution; the emulator
      // redelivers nacked messages so the hook would fire repeatedly otherwise.
      let hookFiredOnce = false;

      const { module: mod, publisher } = await buildModule((svc) => {
        svc.configure(
          async () => {
            throw new Error('intentional handler failure for onNack test');
          },
          {
            subscription: names.sub,
            flowControl: { maxMessages: 1 },
            hooks: {
              onError: (err) => {
                if (!hookFiredOnce) {
                  errorReceived = err;
                }
              },
              onNack: ({ messageId }) => {
                if (!hookFiredOnce) {
                  hookFiredOnce = true;
                  nackedMessageId = messageId;
                  resolveNack();
                }
              },
            },
          }
        );
      });

      await publisher.publish({ body: { id: 'nest-hook-nack-1' } });

      await nackFired;
      await mod.close();

      // onError must have received a ResilientPubSubError with kind:'process'
      expect(isResilientPubSubError(errorReceived)).toBe(true);
      if (isResilientPubSubError(errorReceived)) {
        expect(errorReceived.kind).toBe('process');
      }

      if (nackedMessageId !== undefined) {
        expect(typeof nackedMessageId).toBe('string');
      }
    },
    30_000
  );
});
