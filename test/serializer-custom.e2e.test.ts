/**
 * Suite: serializer-custom (NestJS)
 *
 * Verifies that a custom Serializer<T> injected via SubscriberService.configure()
 * round-trips correctly through the NestJS DI layer.
 *
 * The custom serializer is a base64-wrapped JSON format with content-type
 * 'application/x-base64json'. The test asserts:
 *   1. Round-trip: the received body equals the published body (subscriber
 *      configured with the custom serializer through SubscriberService).
 *   2. Content-type attribute: raw pull via v1.SubscriberClient confirms the
 *      library set `content-type` to the custom serializer's contentType.
 *
 * The publisher uses createResilientPublisher directly (same pattern as the
 * sibling Nest suites). The subscriber is wired through SubscriberService +
 * TestingModule to prove serializer pass-through in the Nest DI layer.
 */

import 'reflect-metadata';
import { describe, it, beforeAll, afterAll, expect } from 'vitest';
import { Test } from '@nestjs/testing';
import type { TestingModule } from '@nestjs/testing';
import { PubSub, v1 } from '@google-cloud/pubsub';
import { createResilientPublisher } from 'resilient-pubsub/publisher';
import type { ResilientPublisher } from 'resilient-pubsub/publisher';
import type { Serializer } from 'resilient-pubsub/envelope';
import { SerializationError } from 'resilient-pubsub/envelope';
import {
  PROJECT_ID,
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

interface UserProfile {
  userId: string;
  email: string;
}

// ── Custom serializer ────────────────────────────────────────────────────────

/**
 * Base64-encoded JSON serializer.
 *
 * serialize:   JSON.stringify → UTF-8 bytes → base64 string → UTF-8 bytes
 * deserialize: UTF-8 bytes → base64 string → original JSON string → parse
 *
 * Content-type: 'application/x-base64json'
 */
const base64JsonSerializer: Serializer<UserProfile> = {
  contentType: 'application/x-base64json',

  serialize(body: UserProfile): Uint8Array {
    const json = JSON.stringify(body);
    const b64 = Buffer.from(json, 'utf8').toString('base64');
    return Buffer.from(b64, 'utf8');
  },

  deserialize(data: Uint8Array): UserProfile {
    const b64 = Buffer.from(data).toString('utf8');
    let json: string;
    try {
      json = Buffer.from(b64, 'base64').toString('utf8');
    } catch (cause) {
      throw new SerializationError('Failed to base64-decode message payload', cause);
    }
    try {
      return JSON.parse(json) as UserProfile;
    } catch (cause) {
      throw new SerializationError('Failed to JSON-parse base64-decoded payload', cause);
    }
  },
};

// ── Fixture ──────────────────────────────────────────────────────────────────

const names = uniqueNames('nest-custom-ser');
let adminClient: PubSub;
let module: TestingModule;
let publisher: ResilientPublisher<UserProfile>;
let subscriberService: SubscriberService<UserProfile>;
let subFqn: string;

let resolveBody!: (body: UserProfile) => void;
const bodyPromise = new Promise<UserProfile>((resolve) => {
  resolveBody = resolve;
});

beforeAll(async () => {
  adminClient = createClient();
  subFqn = `projects/${PROJECT_ID}/subscriptions/${names.sub}`;
  await ensureTopic(adminClient, names.topic);
  await ensureSubscription(adminClient, names.topic, names.sub);

  module = await Test.createTestingModule({
    imports: [PubSubModule],
  }).compile();

  const pubSubClient = module.get<PubSub>(PUBSUB_CLIENT);

  subscriberService = module.get<SubscriberService<UserProfile>>(
    SubscriberService as never
  );

  // Inject the custom serializer through SubscriberService.configure() —
  // proves that serializer option flows through the NestJS DI layer correctly.
  subscriberService.configure(
    async ({ body }) => {
      resolveBody(body);
    },
    {
      subscription: names.sub,
      serializer: base64JsonSerializer,
      flowControl: { maxMessages: 1 },
    }
  );

  await module.init();

  publisher = createResilientPublisher<UserProfile>({
    topic: names.topic,
    pubSubClient,
    serializer: base64JsonSerializer,
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

describe('NestJS custom serializer round-trip', () => {
  it(
    'delivers the body correctly through a custom base64json serializer via SubscriberService',
    async () => {
      const payload: UserProfile = { userId: 'nest-u-001', email: 'nest-test@example.com' };

      await publisher.publish({ body: payload });

      // SubscriberService decodes the body using the injected custom serializer
      const received = await bodyPromise;

      expect(received.userId).toBe(payload.userId);
      expect(received.email).toBe(payload.email);
    },
    30_000
  );

  it(
    'sets content-type attribute to the custom serializer contentType on the wire',
    async () => {
      const payload: UserProfile = { userId: 'nest-u-002', email: 'nest-attr@example.com' };

      await publisher.publish({ body: payload });

      // Use the low-level v1 SubscriberClient to inspect raw attributes without
      // going through the resilient subscriber — verifies the wire format.
      const rawConfig = await adminClient.getClientConfig();
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      const rawClient = new v1.SubscriberClient(rawConfig as any);

      const POLL_INTERVAL_MS = 500;
      const POLL_TIMEOUT_MS = 15_000;
      const deadline = Date.now() + POLL_TIMEOUT_MS;

      let contentTypeAttribute: string | undefined;

      try {
        while (Date.now() < deadline && contentTypeAttribute === undefined) {
          const [response] = await rawClient.pull({
            subscription: subFqn,
            maxMessages: 1,
            returnImmediately: true,
          });

          const messages = response.receivedMessages ?? [];

          if (messages.length > 0) {
            const msg = messages[0]!;
            const attrs = msg.message?.attributes ?? {};
            contentTypeAttribute = (attrs as Record<string, string>)['content-type'];

            // Acknowledge so the message does not redeliver and interfere with
            // the first test's bodyPromise (which has already resolved by now).
            if (msg.ackId) {
              await rawClient.acknowledge({
                subscription: subFqn,
                ackIds: [msg.ackId],
              });
            }
          } else {
            await new Promise<void>((r) => setTimeout(r, POLL_INTERVAL_MS));
          }
        }
      } finally {
        await rawClient.close();
      }

      expect(contentTypeAttribute).toBe('application/x-base64json');
    },
    30_000
  );
});
