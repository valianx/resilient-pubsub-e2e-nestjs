/**
 * Suite: poison (NestJS)
 *
 * Verifies the poison-message path through the NestJS DI layer.
 *
 * Strategy:
 *   - Configure SubscriberService with the default JSON serializer and an
 *     onPoison hook passed via SubscriberService.configure().
 *   - Publish raw non-JSON bytes via the native topic.publishMessage() API
 *     to bypass the resilient publisher's serializer — controls bytes exactly.
 *   - Assert: onPoison fires with an error, the handler is NOT invoked, and
 *     onNack does NOT fire (only onPoison fires on deserialization failure).
 *
 * The emulator redelivers nacked messages. The subscriber is stopped as soon
 * as onPoison fires to prevent the test from running indefinitely.
 */

import 'reflect-metadata';
import { describe, it, beforeAll, afterAll, expect } from 'vitest';
import { Test } from '@nestjs/testing';
import type { TestingModule } from '@nestjs/testing';
import { PubSub } from '@google-cloud/pubsub';
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

interface ValidPayload {
  value: string;
}

// ── Fixture ──────────────────────────────────────────────────────────────────

const names = uniqueNames('nest-poison');
let adminClient: PubSub;
let module: TestingModule;
let subscriberService: SubscriberService<ValidPayload>;

beforeAll(async () => {
  adminClient = createClient();
  await ensureTopic(adminClient, names.topic);
  await ensureSubscription(adminClient, names.topic, names.sub);

  module = await Test.createTestingModule({
    imports: [PubSubModule],
  }).compile();
});

afterAll(async () => {
  // module.close() calls onApplicationShutdown → subscriber.stop() for drain
  await module.close();
  await deleteSub(adminClient, names.sub);
  await deleteTopic(adminClient, names.topic);
  await adminClient.close();
});

// ── Tests ────────────────────────────────────────────────────────────────────

describe('NestJS poison message detection', () => {
  it(
    'fires onPoison via SubscriberService hooks and does not invoke the handler',
    async () => {
      let handlerInvoked = false;
      let onNackInvoked = false;
      let poisonMessageId: string | undefined;
      let poisonError: unknown;

      let resolvePoisoned!: () => void;
      const poisonedPromise = new Promise<void>((resolve) => {
        resolvePoisoned = resolve;
      });

      subscriberService = module.get<SubscriberService<ValidPayload>>(
        SubscriberService as never
      );

      // Wire hooks through SubscriberService.configure() — proves hook option
      // flows correctly through the NestJS DI layer.
      subscriberService.configure(
        async () => {
          // This handler MUST NOT be called for a poison message
          handlerInvoked = true;
        },
        {
          subscription: names.sub,
          // Default JsonSerializer — will throw on non-JSON bytes
          flowControl: { maxMessages: 1 },
          hooks: {
            onPoison: ({ messageId, error }) => {
              poisonMessageId = messageId;
              poisonError = error;
              resolvePoisoned();
            },
            onNack: () => {
              // onNack is NOT expected for deserialization failures —
              // only onPoison fires on that path
              onNackInvoked = true;
            },
          },
        }
      );

      await module.init();

      // Publish raw garbage bytes using the native API — bypasses the resilient
      // publisher's serializer so we control the wire bytes exactly.
      const pubSubClient = module.get<PubSub>(PUBSUB_CLIENT);
      const topic = pubSubClient.topic(names.topic);
      await topic.publishMessage({
        data: Buffer.from('this-is-not-valid-json-\x00\x01\x02', 'binary'),
      });

      // Wait for onPoison to fire
      await poisonedPromise;

      // Give a short window to confirm the handler does not fire asynchronously
      await new Promise<void>((r) => setTimeout(r, 300));

      // Core assertions
      expect(handlerInvoked).toBe(false);
      expect(onNackInvoked).toBe(false);
      expect(poisonError).toBeDefined();

      // messageId is optional — the emulator may or may not set it
      if (poisonMessageId !== undefined) {
        expect(typeof poisonMessageId).toBe('string');
      }
    },
    30_000
  );
});
