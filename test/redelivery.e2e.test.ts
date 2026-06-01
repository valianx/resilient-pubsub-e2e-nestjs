/**
 * Suite: redelivery (NestJS)
 *
 * Verifies the at-least-once delivery contract through the NestJS DI layer
 * without a dead-letter policy:
 *
 *   1. The handler (wired via SubscriberService) throws on the first delivery
 *      → message is nacked.
 *   2. The emulator redelivers the message.
 *   3. The handler succeeds on the second delivery → message is acked.
 *
 * This proves the nack→redeliver→reprocess path (the fundamental at-least-once
 * guarantee) is preserved when the subscriber runs inside Nest's DI container.
 *
 * Implementation:
 *   - A delivery counter in the Nest-wired handler tracks attempts.
 *   - Attempt 1: throw → nack.
 *   - Attempt 2+: resolve → ack → signal the test.
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

interface RetryableEvent {
  taskId: string;
}

// ── Fixture ──────────────────────────────────────────────────────────────────

const names = uniqueNames('nest-redelivery');
let adminClient: PubSub;
let module: TestingModule;
let publisher: ResilientPublisher<RetryableEvent>;
let subscriberService: SubscriberService<RetryableEvent>;

let deliveryCount = 0;
let successfulDeliveryCount = 0;

let resolveSuccess!: () => void;
const successPromise = new Promise<void>((resolve) => {
  resolveSuccess = resolve;
});

beforeAll(async () => {
  adminClient = createClient();
  await ensureTopic(adminClient, names.topic);
  // Plain subscription — no dead-letter policy
  await ensureSubscription(adminClient, names.topic, names.sub);

  module = await Test.createTestingModule({
    imports: [PubSubModule],
  }).compile();

  const pubSubClient = module.get<PubSub>(PUBSUB_CLIENT);

  subscriberService = module.get<SubscriberService<RetryableEvent>>(
    SubscriberService as never
  );

  subscriberService.configure(
    async ({ body }) => {
      deliveryCount++;

      if (deliveryCount === 1) {
        // First delivery: throw to trigger nack → redeliver
        throw new Error(`[nest-redelivery] First delivery of ${body.taskId} — intentional nack`);
      }

      // Second delivery (or later): succeed → ack
      successfulDeliveryCount++;
      resolveSuccess();
    },
    {
      subscription: names.sub,
      // Tight flow control: process one message at a time so redeliveries
      // arrive in a controlled manner without queue build-up
      flowControl: { maxMessages: 1 },
    }
  );

  await module.init();

  publisher = createResilientPublisher<RetryableEvent>({
    topic: names.topic,
    pubSubClient,
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

describe('NestJS at-least-once redelivery', () => {
  it(
    'redelivers and eventually acks a nacked message through SubscriberService (nack → redeliver → success)',
    async () => {
      await publisher.publish({ body: { taskId: 'nest-task-001' } });

      // Wait for the message to be successfully processed after redelivery
      await successPromise;

      // At least 2 deliveries: the initial nack + the successful redeliver
      expect(deliveryCount).toBeGreaterThanOrEqual(2);
      // Exactly one successful (acked) delivery
      expect(successfulDeliveryCount).toBe(1);
    },
    30_000
  );
});
