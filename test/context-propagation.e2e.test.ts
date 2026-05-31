/**
 * Suite: context-propagation (NestJS)
 *
 * Verifies the allowlist-gated propagation model through the Nest DI layer:
 *   - `traceparent` propagates automatically (always in allowlist).
 *   - `x-tenant-id` propagates because it is explicitly allowlisted.
 *   - `x-secret` is DROPPED because it is NOT on the allowlist.
 *
 * Both publisher and subscriber must use the same propagation options for the
 * round-trip to be symmetric (as documented in the library).
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

interface Ping {
  id: string;
}

// ── Fixture ──────────────────────────────────────────────────────────────────

const names = uniqueNames('nest-ctx-prop');
const PROPAGATION_OPTS = { allowlist: ['x-tenant-id'] };
const TRACE_PARENT = '00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01';

let adminClient: PubSub;
let module: TestingModule;
let publisher: ResilientPublisher<Ping>;
let subscriberService: SubscriberService<Ping>;

let resolveHeaders!: (h: Record<string, string>) => void;
const headersPromise = new Promise<Record<string, string>>((resolve) => {
  resolveHeaders = resolve;
});

beforeAll(async () => {
  adminClient = createClient();
  await ensureTopic(adminClient, names.topic);
  await ensureSubscription(adminClient, names.topic, names.sub);

  module = await Test.createTestingModule({
    imports: [PubSubModule],
  }).compile();

  const pubSubClient = module.get<PubSub>(PUBSUB_CLIENT);

  subscriberService = module.get<SubscriberService<Ping>>(
    SubscriberService as never
  );
  subscriberService.configure(
    async ({ headers }) => {
      resolveHeaders(headers);
    },
    {
      subscription: names.sub,
      propagation: PROPAGATION_OPTS,
      flowControl: { maxMessages: 1 },
    }
  );

  await module.init();

  publisher = createResilientPublisher<Ping>({
    topic: names.topic,
    pubSubClient,
    propagation: PROPAGATION_OPTS,
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

describe('NestJS context propagation with allowlist', () => {
  it('delivers traceparent and x-tenant-id but drops x-secret', async () => {
    await publisher.publish({
      body: { id: 'nest-ping-1' },
      headers: {
        traceparent: TRACE_PARENT,
        'x-tenant-id': 'nest-acme-corp',
        'x-secret': 'super-secret-value',
      },
    });

    const headers = await headersPromise;

    // W3C trace header must propagate automatically
    expect(headers['traceparent']).toBe(TRACE_PARENT);

    // Allowlisted business header must propagate
    expect(headers['x-tenant-id']).toBe('nest-acme-corp');

    // Non-allowlisted header must be absent
    expect(headers['x-secret']).toBeUndefined();
  });
});
