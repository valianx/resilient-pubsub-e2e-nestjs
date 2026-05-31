/**
 * Shared test harness for resilient-pubsub-e2e-nestjs.
 *
 * Mirrors the plain-Node e2e harness exactly — same helpers, same patterns —
 * so the NestJS test suites exercise the library under identical conditions.
 *
 * Provides:
 * - PubSub client pointed at the emulator via PUBSUB_EMULATOR_HOST
 * - Unique resource name generation per suite (avoids cross-test interference)
 * - Idempotent topic + subscription creation (ignores ALREADY_EXISTS / gRPC 6)
 * - Best-effort cleanup helpers for afterAll hooks
 *
 * Environment variables:
 *   PUBSUB_EMULATOR_HOST  — required; e.g. "localhost:8085"
 *   PROJECT_ID            — optional; defaults to "e2e-project"
 */

import { PubSub } from '@google-cloud/pubsub';

// ── Environment ──────────────────────────────────────────────────────────────

export const PROJECT_ID = process.env['PROJECT_ID'] ?? 'e2e-project';

/**
 * gRPC status code for ALREADY_EXISTS.
 * The emulator returns this when a topic or subscription already exists.
 */
const GRPC_ALREADY_EXISTS = 6;

// ── Client factory ───────────────────────────────────────────────────────────

/**
 * Creates a PubSub client that targets the emulator.
 *
 * @google-cloud/pubsub auto-detects PUBSUB_EMULATOR_HOST and skips auth.
 */
export function createClient(): PubSub {
  return new PubSub({ projectId: PROJECT_ID });
}

// ── Unique name generation ───────────────────────────────────────────────────

let counter = 0;

/**
 * Returns unique topic and subscription names for a test suite.
 * Appending a counter prevents cross-suite topic/subscription name collisions
 * when all suites run in the same process.
 */
export function uniqueNames(prefix: string): { topic: string; sub: string } {
  const id = ++counter;
  return {
    topic: `${prefix}-topic-${id}`,
    sub: `${prefix}-sub-${id}`,
  };
}

/**
 * Returns a unique dead-letter topic name and its companion pull subscription.
 */
export function uniqueDlqNames(prefix: string): {
  dlqTopic: string;
  dlqSub: string;
} {
  const id = ++counter;
  return {
    dlqTopic: `${prefix}-dlq-topic-${id}`,
    dlqSub: `${prefix}-dlq-sub-${id}`,
  };
}

// ── Resource creation helpers ────────────────────────────────────────────────

/**
 * Creates a topic if it does not already exist.
 * Ignores ALREADY_EXISTS (gRPC 6) for test idempotency.
 */
export async function ensureTopic(
  client: PubSub,
  topicName: string
): Promise<void> {
  try {
    await client.createTopic(topicName);
  } catch (err) {
    if (isAlreadyExists(err)) return;
    throw err;
  }
}

/**
 * Creates a subscription if it does not already exist.
 * Accepts an optional options object (e.g. for deadLetterPolicy).
 * Ignores ALREADY_EXISTS (gRPC 6) for test idempotency.
 */
export async function ensureSubscription(
  client: PubSub,
  topicName: string,
  subName: string,
  options?: Record<string, unknown>
): Promise<void> {
  try {
    await client.createSubscription(topicName, subName, options ?? {});
  } catch (err) {
    if (isAlreadyExists(err)) return;
    throw err;
  }
}

// ── Cleanup helpers ──────────────────────────────────────────────────────────

/**
 * Deletes a subscription; swallows all errors (best-effort cleanup).
 */
export async function deleteSub(client: PubSub, subName: string): Promise<void> {
  try {
    await client.subscription(subName).delete();
  } catch {
    // best-effort — do not fail the test suite on cleanup errors
  }
}

/**
 * Deletes a topic; swallows all errors (best-effort cleanup).
 */
export async function deleteTopic(client: PubSub, topicName: string): Promise<void> {
  try {
    await client.topic(topicName).delete();
  } catch {
    // best-effort — do not fail the test suite on cleanup errors
  }
}

// ── Internal helpers ─────────────────────────────────────────────────────────

function isAlreadyExists(err: unknown): boolean {
  if (err === null || typeof err !== 'object') return false;
  const code = (err as Record<string, unknown>)['code'];
  return code === GRPC_ALREADY_EXISTS;
}
