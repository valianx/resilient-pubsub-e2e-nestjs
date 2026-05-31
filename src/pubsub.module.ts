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
import { PUBSUB_CLIENT } from './tokens.js';

// Tokens are defined in ./tokens.ts (standalone, no imports) to avoid a circular
// import between this module and subscriber.service.ts that would break DI.
// Re-export for convenience so existing imports of these names keep working.
export { PUBSUB_CLIENT, RESILIENT_PUBLISHER } from './tokens.js';

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
