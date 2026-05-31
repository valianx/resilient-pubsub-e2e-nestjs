/**
 * Suite: graceful-shutdown (NestJS)
 *
 * THE Nest-native showcase — proves that OnApplicationShutdown wires the
 * library's drain semantics into the Nest lifecycle automatically.
 *
 * Scenario:
 *   1. Bootstrap the full Nest application context.
 *   2. Publish a message and wait until the handler has started its slow work
 *      (signaled via a promise resolved at the top of the handler).
 *   3. Call app.close() — Nest fires OnApplicationShutdown → SubscriberService
 *      calls subscriber.stop() → the library drains the in-flight handler.
 *   4. Assert app.close() resolves AFTER the handler has completed.
 *   5. Assert the handler actually finished its work (handlerCompletedAt is set).
 *
 * This is the primary advantage over plain-Node usage: the consumer does NOT
 * need to wire SIGTERM → subscriber.stop() manually; Nest handles it via the
 * lifecycle hook contract.
 *
 * stopTimeoutMs is set well above HANDLER_DELAY_MS so the graceful-drain path
 * (not the timeout-nack path) is exercised.
 */

import 'reflect-metadata';
import { describe, it, beforeAll, afterAll, expect } from 'vitest';
import { Test } from '@nestjs/testing';
import type { TestingModule } from '@nestjs/testing';
import { PubSub } from '@google-cloud/pubsub';
import { createResilientPublisher } from 'resilient-pubsub/publisher';
import type { ResilientPublisher } from 'resilient-pubsub/publisher';
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

interface SlowPayload {
  seq: number;
}

// ── Constants ────────────────────────────────────────────────────────────────

/** How long the in-flight handler sleeps before resolving. */
const HANDLER_DELAY_MS = 1_000;

/** stop() timeout — longer than handler delay so graceful drain runs. */
const STOP_TIMEOUT_MS = 10_000;

// ── Fixture ──────────────────────────────────────────────────────────────────

const names = uniqueNames('nest-graceful');
let adminClient: PubSub;
let module: TestingModule;
let publisher: ResilientPublisher<SlowPayload>;

beforeAll(async () => {
  adminClient = createClient();
  await ensureTopic(adminClient, names.topic);
  await ensureSubscription(adminClient, names.topic, names.sub);
});

afterAll(async () => {
  await deleteSub(adminClient, names.sub);
  await deleteTopic(adminClient, names.topic);
  await adminClient.close();
});

// ── Tests ────────────────────────────────────────────────────────────────────

describe('NestJS graceful shutdown via OnApplicationShutdown', () => {
  it(
    'app.close() drains the in-flight handler before resolving',
    async () => {
      // Track handler lifecycle with timestamps
      let handlerStartedAt: number | undefined;
      let handlerCompletedAt: number | undefined;

      // Signals that the handler has started its slow work
      let signalHandlerStarted!: () => void;
      const handlerStartedPromise = new Promise<void>((resolve) => {
        signalHandlerStarted = resolve;
      });

      // Build a fresh Nest module for this test.
      module = await Test.createTestingModule({
        imports: [PubSubModule],
      }).compile();

      // enableShutdownHooks() is required so Nest fires OnApplicationShutdown
      // when module.close() is called — without it the hook is never triggered.
      module.enableShutdownHooks();

      const pubSubClient = module.get<PubSub>(PUBSUB_CLIENT);

      const subscriberService = module.get<SubscriberService<SlowPayload>>(
        SubscriberService as never
      );

      subscriberService.configure(
        async () => {
          handlerStartedAt = Date.now();
          signalHandlerStarted();

          // Simulate slow work to give app.close() time to be called mid-flight.
          await new Promise<void>((r) => setTimeout(r, HANDLER_DELAY_MS));

          handlerCompletedAt = Date.now();
        },
        {
          subscription: names.sub,
          stopTimeoutMs: STOP_TIMEOUT_MS,
          flowControl: { maxMessages: 1 },
        }
      );

      await module.init();

      publisher = createResilientPublisher<SlowPayload>({
        topic: names.topic,
        pubSubClient,
        retry: { maxAttempts: 3, initialDelay: 100, jitter: 'none' },
      });

      // Give the subscriber a moment to attach before publishing.
      await new Promise<void>((r) => setTimeout(r, 200));

      await publisher.publish({ body: { seq: 1 } });

      // Wait until the handler has actually started processing.
      await handlerStartedPromise;

      // Trigger Nest shutdown while the handler is still sleeping.
      // OnApplicationShutdown → SubscriberService.onApplicationShutdown()
      //   → subscriber.stop() → waits for in-flight handler to complete.
      const closeStartedAt = Date.now();
      await module.close();
      const closeResolvedAt = Date.now();

      // The handler must have started before shutdown was triggered.
      expect(handlerStartedAt).toBeDefined();

      // The handler must have completed — graceful drain waited for it.
      expect(handlerCompletedAt).toBeDefined();

      // module.close() must have resolved AFTER the handler finished.
      // Add 50 ms tolerance for timing jitter.
      expect(closeResolvedAt).toBeGreaterThanOrEqual(
        (handlerCompletedAt as number) - 50
      );

      // Shutdown must not have completed before the handler even started
      // (sanity check that the handler actually ran inside the drain window).
      expect(closeStartedAt).toBeLessThanOrEqual(handlerCompletedAt as number);

      // The handler delay must be measurable.
      const elapsed =
        (handlerCompletedAt as number) - (handlerStartedAt as number);
      expect(elapsed).toBeGreaterThanOrEqual(HANDLER_DELAY_MS - 100);
    },
    // Give this test a ceiling above HANDLER_DELAY_MS + STOP_TIMEOUT_MS.
    STOP_TIMEOUT_MS + HANDLER_DELAY_MS + 5_000
  );
});
