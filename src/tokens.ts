/**
 * Injection tokens for the resilient-pubsub NestJS integration.
 *
 * These live in their OWN module with no other imports so that neither
 * `pubsub.module.ts` nor `subscriber.service.ts` import from each other for the
 * token. A circular import (module -> service -> module) would leave the token
 * `undefined` at the moment the `@Inject(...)` decorator runs, and Nest would
 * then fall back to the constructor param's design type and throw
 * "can't resolve dependencies of the SubscriberService (?) ... Function at
 * index [0]". Keeping tokens standalone breaks that cycle.
 */

/** Injection token for the shared @google-cloud/pubsub PubSub client. */
export const PUBSUB_CLIENT = 'PUBSUB_CLIENT';

/** Injection token for the resilient publisher factory instance. */
export const RESILIENT_PUBLISHER = 'RESILIENT_PUBLISHER';
