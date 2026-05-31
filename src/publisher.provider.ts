/**
 * publisher.provider.ts
 *
 * Factory function that creates a NestJS FactoryProvider wrapping
 * createResilientPublisher for a given topic.
 *
 * Usage:
 *   providers: [makePublisherProvider<OrderCreated>('orders-topic', PUBLISHER_TOKEN)]
 *
 * The resulting provider is injectable by token and wraps the library's
 * ResilientPublisher with all its retry / backoff / propagation semantics,
 * powered by the shared PubSub client from PUBSUB_CLIENT.
 */

import type { FactoryProvider } from '@nestjs/common';
import type { PubSub } from '@google-cloud/pubsub';
import { createResilientPublisher } from 'resilient-pubsub/publisher';
import type { ResilientPublisher, PublisherOptions } from 'resilient-pubsub/publisher';
import { PUBSUB_CLIENT } from './pubsub.module.js';

/**
 * Builds a NestJS FactoryProvider that produces a ResilientPublisher<T>.
 *
 * @param topic     - The Pub/Sub topic name (short or fully-qualified).
 * @param token     - The injection token callers use to resolve this publisher.
 * @param options   - Optional overrides for retry / propagation / hooks.
 */
export function makePublisherProvider<T>(
  topic: string,
  token: string | symbol,
  options?: Omit<PublisherOptions<T>, 'topic' | 'pubSubClient'>
): FactoryProvider<ResilientPublisher<T>> {
  return {
    provide: token,
    inject: [PUBSUB_CLIENT],
    useFactory: (pubSubClient: PubSub): ResilientPublisher<T> => {
      return createResilientPublisher<T>({
        topic,
        pubSubClient,
        ...options,
      });
    },
  };
}
