/**
 * Suite: publish-subscribe (NestJS)
 *
 * Verifies the fundamental round-trip through the NestJS DI layer:
 *   NestJS TestingModule boots → publisher provider resolves → subscriber
 *   service starts via OnModuleInit → publish → handler (wired as a Nest
 *   provider) receives the typed body → ack.
 *
 * This proves the library works identically inside the Nest DI container as
 * it does in plain Node: same API, same semantics, framework-agnostic.
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

interface OrderCreated {
  orderId: string;
  amount: number;
}

// ── Fixture ──────────────────────────────────────────────────────────────────

const names = uniqueNames('nest-pub-sub');
let adminClient: PubSub;
let module: TestingModule;
let publisher: ResilientPublisher<OrderCreated>;
let subscriberService: SubscriberService<OrderCreated>;

// Promise settled by the handler when the expected message arrives.
let resolveReceived!: (msg: OrderCreated) => void;
const receivedPromise = new Promise<OrderCreated>((resolve) => {
  resolveReceived = resolve;
});

beforeAll(async () => {
  adminClient = createClient();
  await ensureTopic(adminClient, names.topic);
  await ensureSubscription(adminClient, names.topic, names.sub);

  module = await Test.createTestingModule({
    imports: [PubSubModule],
  }).compile();

  const pubSubClient = module.get<PubSub>(PUBSUB_CLIENT);

  // Wire the subscriber service before the module initialises.
  subscriberService = module.get<SubscriberService<OrderCreated>>(
    SubscriberService as never
  );
  subscriberService.configure(
    async ({ body }) => {
      resolveReceived(body);
    },
    {
      subscription: names.sub,
      flowControl: { maxMessages: 1 },
    }
  );

  // Initialise the module — triggers OnModuleInit → subscriber.start()
  await module.init();

  publisher = createResilientPublisher<OrderCreated>({
    topic: names.topic,
    pubSubClient,
    retry: { maxAttempts: 3, initialDelay: 100, jitter: 'none' },
  });
});

afterAll(async () => {
  // Close the Nest app — triggers OnApplicationShutdown → subscriber.stop()
  await module.close();
  await deleteSub(adminClient, names.sub);
  await deleteTopic(adminClient, names.topic);
  await adminClient.close();
});

// ── Tests ────────────────────────────────────────────────────────────────────

describe('NestJS publish → subscribe round-trip', () => {
  it('delivers the published body to the subscriber handler via Nest DI', async () => {
    const body: OrderCreated = { orderId: 'nest-ord-001', amount: 99.99 };

    const result = await publisher.publish({ body });
    expect(typeof result.messageId).toBe('string');
    expect(result.messageId.length).toBeGreaterThan(0);

    // Wait for the handler to fire (30 s vitest timeout covers this).
    const received = await receivedPromise;
    expect(received.orderId).toBe(body.orderId);
    expect(received.amount).toBe(body.amount);
  });
});
