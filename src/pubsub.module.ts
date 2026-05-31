/**
 * PubSubModule — NestJS module that wires up the shared PubSub client and
 * exposes injection tokens for resilient publishers and subscribers.
 *
 * The module reads PUBSUB_EMULATOR_HOST from the environment, which makes
 * @google-cloud/pubsub connect to the local emulator automatically (same
 * mechanism as the plain-Node e2e repo).
 *
 * The shared PubSub client is provided as PUBSUB_CLIENT so individual
 * publisher providers and subscriber services can inject it by token.
 */

import 'reflect-metadata';
import { Module } from '@nestjs/common';
import { PubSub } from '@google-cloud/pubsub';
import { SubscriberService } from './subscriber.service.js';

/** Injection token for the shared @google-cloud/pubsub PubSub client. */
export const PUBSUB_CLIENT = 'PUBSUB_CLIENT';

/** Injection token for the resilient publisher factory instance. */
export const RESILIENT_PUBLISHER = 'RESILIENT_PUBLISHER';

/**
 * Environment variable read by @google-cloud/pubsub to locate the emulator.
 * When set (e.g. "localhost:8085"), the SDK bypasses auth and routes all
 * calls to the emulator endpoint.
 */
const PROJECT_ID = process.env['PROJECT_ID'] ?? 'e2e-project';

@Module({
  providers: [
    {
      provide: PUBSUB_CLIENT,
      useFactory: (): PubSub => new PubSub({ projectId: PROJECT_ID }),
    },
    SubscriberService,
  ],
  exports: [PUBSUB_CLIENT, SubscriberService],
})
export class PubSubModule {}
