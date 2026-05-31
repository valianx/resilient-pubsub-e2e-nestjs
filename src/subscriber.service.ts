/**
 * subscriber.service.ts
 *
 * Injectable NestJS service that wraps createResilientSubscriber.
 *
 * Lifecycle hooks:
 *   OnModuleInit          — calls subscriber.start() to begin consuming messages.
 *   OnApplicationShutdown — calls subscriber.stop() for graceful drain on app close.
 *
 * This is the primary NestJS-native demonstration of the library's graceful-
 * shutdown feature: when Nest closes the application (e.g. app.close() in tests,
 * or SIGTERM in production), OnApplicationShutdown fires, and stop() drains all
 * in-flight message handlers before the process exits.
 *
 * Design note: the service is intentionally configured dynamically via
 * configure() so tests can inject topic/subscription names and handlers without
 * requiring a fully wired NestJS DI tree for every test variant. In production
 * code one would inject ConfigService here instead.
 */

import 'reflect-metadata';
import {
  Injectable,
  Inject,
  OnModuleInit,
  OnApplicationShutdown,
} from '@nestjs/common';
import type { PubSub } from '@google-cloud/pubsub';
import { createResilientSubscriber } from 'resilient-pubsub/subscriber';
import type {
  ResilientSubscriber,
  SubscriberOptions,
  MessageHandler,
} from 'resilient-pubsub/subscriber';
import { PUBSUB_CLIENT } from './tokens.js';

@Injectable()
export class SubscriberService<T = unknown>
  implements OnModuleInit, OnApplicationShutdown
{
  private subscriber: ResilientSubscriber<T> | undefined;
  private handler: MessageHandler<T> | undefined;
  private options: Omit<SubscriberOptions<T>, 'pubSubClient'> | undefined;

  constructor(@Inject(PUBSUB_CLIENT) private readonly pubSubClient: PubSub) {}

  /**
   * Configures the service with the subscription name, message handler, and
   * optional subscriber options. Must be called before onModuleInit fires (i.e.,
   * before the module is fully initialized).
   *
   * This method is intentionally synchronous so tests can call it in a
   * beforeAll hook before TestingModule.compile() completes.
   */
  configure(
    handler: MessageHandler<T>,
    options: Omit<SubscriberOptions<T>, 'pubSubClient'>
  ): void {
    this.handler = handler;
    this.options = options;
  }

  /**
   * NestJS lifecycle hook — called after all providers are wired.
   * Builds the subscriber and starts consuming messages.
   */
  onModuleInit(): void {
    if (this.options === undefined || this.handler === undefined) {
      // No subscription configured for this module instance — skip.
      // This allows the module to be imported in contexts where a subscription
      // is not yet needed (e.g., publish-only test modules).
      return;
    }

    this.subscriber = createResilientSubscriber<T>({
      ...this.options,
      pubSubClient: this.pubSubClient,
    });

    this.subscriber.on(this.handler);
    this.subscriber.start();
  }

  /**
   * NestJS lifecycle hook — called when the application is shutting down.
   * Waits for in-flight message handlers to complete (graceful drain) before
   * allowing the process to exit.
   *
   * This is the critical integration point: Nest calls onApplicationShutdown
   * automatically when app.close() is invoked, so the library's drain semantics
   * are exercised as a natural part of the Nest lifecycle — no manual wiring
   * required in consumer code.
   */
  async onApplicationShutdown(_signal?: string): Promise<void> {
    if (this.subscriber !== undefined) {
      await this.subscriber.stop();
    }
  }

  /**
   * Exposes the underlying ResilientSubscriber for tests that need direct access
   * (e.g., to call stop() manually before app.close()).
   */
  get resilientSubscriber(): ResilientSubscriber<T> | undefined {
    return this.subscriber;
  }
}
