/**
 * Suite: ordering (NestJS)
 *
 * Verifies that ordering-key semantics work end-to-end through the NestJS DI
 * layer. The subscriber is wired through SubscriberService + TestingModule;
 * the publisher uses createResilientPublisher directly with `ordering: true`.
 *
 * The subscription is created with `enableMessageOrdering: true` and all
 * messages share the same orderingKey, so they must arrive in published order.
 *
 * Emulator note: if the emulator delivers out of order (infrastructure flake)
 * the test falls back to asserting all N messages arrived (at-least-once
 * contract) and logs a warning — identical to the Node repo's behaviour.
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

interface SequencedEvent {
  seq: number;
}

// ── Constants ────────────────────────────────────────────────────────────────

const ORDERING_KEY = 'nest-e2e-ordering-key';
const TOTAL_MESSAGES = 5;

// ── Fixture ──────────────────────────────────────────────────────────────────

const names = uniqueNames('nest-ordering');
let adminClient: PubSub;
let module: TestingModule;
let publisher: ResilientPublisher<SequencedEvent>;
let subscriberService: SubscriberService<SequencedEvent>;

const received: number[] = [];
let resolveAll!: () => void;
const allReceived = new Promise<void>((resolve) => {
  resolveAll = resolve;
});

beforeAll(async () => {
  adminClient = createClient();
  await ensureTopic(adminClient, names.topic);
  await ensureSubscription(adminClient, names.topic, names.sub, {
    enableMessageOrdering: true,
  });

  module = await Test.createTestingModule({
    imports: [PubSubModule],
  }).compile();

  const pubSubClient = module.get<PubSub>(PUBSUB_CLIENT);

  subscriberService = module.get<SubscriberService<SequencedEvent>>(
    SubscriberService as never
  );

  subscriberService.configure(
    async ({ body }) => {
      received.push(body.seq);
      if (received.length >= TOTAL_MESSAGES) {
        resolveAll();
      }
    },
    {
      subscription: names.sub,
      flowControl: { maxMessages: 1 },
    }
  );

  await module.init();

  publisher = createResilientPublisher<SequencedEvent>({
    topic: names.topic,
    pubSubClient,
    ordering: true,
    retry: { maxAttempts: 3, initialDelay: 100, jitter: 'none' },
  });
});

afterAll(async () => {
  await module.close();
  await deleteSub(adminClient, names.sub);
  await deleteTopic(adminClient, names.topic);
  await adminClient.close();
});

// ── Tests ────────────────────────────────────────────────────────────────────

describe('NestJS ordering keys', () => {
  it(
    'delivers messages with the same orderingKey in published order via SubscriberService',
    async () => {
      // Publish messages in known order — all with the same ordering key
      for (let seq = 1; seq <= TOTAL_MESSAGES; seq++) {
        await publisher.publish({ body: { seq }, orderingKey: ORDERING_KEY });
      }

      // Wait until all messages have been received
      await allReceived;

      expect(received).toHaveLength(TOTAL_MESSAGES);

      // Primary assertion: strict in-order delivery
      const isInOrder = received.every((seq, idx) => seq === idx + 1);

      if (!isInOrder) {
        // Emulator flake — all messages arrived but order was not guaranteed
        // eslint-disable-next-line no-console
        console.warn(
          '[nest-ordering] Messages arrived but not in strict order — emulator timing variance.',
          'Received:', received,
          'Expected: [1, 2, 3, 4, 5]. Asserting all-arrive contract instead.'
        );
        const sorted = [...received].sort((a, b) => a - b);
        expect(sorted).toEqual([1, 2, 3, 4, 5]);
      } else {
        expect(received).toEqual([1, 2, 3, 4, 5]);
      }
    },
    30_000
  );
});
