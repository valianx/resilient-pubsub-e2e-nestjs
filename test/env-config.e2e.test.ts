/**
 * Suite: env-config (NestJS)
 *
 * Verifies that RESILIENT_PUBSUB_* environment variables are correctly parsed
 * by the library's configuration resolver.
 *
 * Design rationale:
 *   Forcing a transient failure in the emulator to count onRetry invocations is
 *   unreliable (the emulator classifies errors as permanent / NOT_FOUND, which
 *   the library does not retry). Instead we test the configuration resolver
 *   directly — `resolveConfigFromEnv` accepts a plain object as its `env`
 *   parameter, making the test fully deterministic without touching process.env.
 *
 * This is intentionally a unit-style assertion at the e2e layer: it proves the
 * env-parsing code that runs inside the publisher/subscriber factories works
 * correctly against the real library build (the installed git-dependency), not a
 * mock. No emulator interaction or Nest DI is needed for this suite.
 *
 * Identical in structure and assertions to the Node repo's env-config suite —
 * it validates the same library code from within the NestJS consumer repo.
 */

import { describe, it, expect } from 'vitest';
import { resolveConfigFromEnv } from 'resilient-pubsub/config';

// ── Tests ────────────────────────────────────────────────────────────────────

describe('env-var configuration resolution', () => {
  it('parses RESILIENT_PUBSUB_MAX_ATTEMPTS as a positive integer', () => {
    const cfg = resolveConfigFromEnv({ RESILIENT_PUBSUB_MAX_ATTEMPTS: '7' });
    expect(cfg.maxAttempts).toBe(7);
  });

  it('ignores RESILIENT_PUBSUB_MAX_ATTEMPTS when the value is not a positive integer', () => {
    expect(resolveConfigFromEnv({ RESILIENT_PUBSUB_MAX_ATTEMPTS: '0' }).maxAttempts).toBeUndefined();
    expect(resolveConfigFromEnv({ RESILIENT_PUBSUB_MAX_ATTEMPTS: '-1' }).maxAttempts).toBeUndefined();
    expect(resolveConfigFromEnv({ RESILIENT_PUBSUB_MAX_ATTEMPTS: 'abc' }).maxAttempts).toBeUndefined();
    expect(resolveConfigFromEnv({ RESILIENT_PUBSUB_MAX_ATTEMPTS: '' }).maxAttempts).toBeUndefined();
  });

  it('parses RESILIENT_PUBSUB_BACKOFF_STRATEGY for each valid strategy', () => {
    expect(resolveConfigFromEnv({ RESILIENT_PUBSUB_BACKOFF_STRATEGY: 'exponential' }).strategy).toBe('exponential');
    expect(resolveConfigFromEnv({ RESILIENT_PUBSUB_BACKOFF_STRATEGY: 'linear' }).strategy).toBe('linear');
    expect(resolveConfigFromEnv({ RESILIENT_PUBSUB_BACKOFF_STRATEGY: 'constant' }).strategy).toBe('constant');
  });

  it('ignores RESILIENT_PUBSUB_BACKOFF_STRATEGY for unrecognized values', () => {
    expect(resolveConfigFromEnv({ RESILIENT_PUBSUB_BACKOFF_STRATEGY: 'random' }).strategy).toBeUndefined();
    expect(resolveConfigFromEnv({ RESILIENT_PUBSUB_BACKOFF_STRATEGY: '' }).strategy).toBeUndefined();
  });

  it('parses RESILIENT_PUBSUB_JITTER for each valid jitter strategy', () => {
    expect(resolveConfigFromEnv({ RESILIENT_PUBSUB_JITTER: 'full' }).jitter).toBe('full');
    expect(resolveConfigFromEnv({ RESILIENT_PUBSUB_JITTER: 'equal' }).jitter).toBe('equal');
    expect(resolveConfigFromEnv({ RESILIENT_PUBSUB_JITTER: 'decorrelated' }).jitter).toBe('decorrelated');
    expect(resolveConfigFromEnv({ RESILIENT_PUBSUB_JITTER: 'none' }).jitter).toBe('none');
  });

  it('parses RESILIENT_PUBSUB_INITIAL_DELAY as a positive integer', () => {
    const cfg = resolveConfigFromEnv({ RESILIENT_PUBSUB_INITIAL_DELAY: '500' });
    expect(cfg.initialDelay).toBe(500);
  });

  it('parses RESILIENT_PUBSUB_MAX_DELAY as a positive integer', () => {
    const cfg = resolveConfigFromEnv({ RESILIENT_PUBSUB_MAX_DELAY: '60000' });
    expect(cfg.maxDelay).toBe(60000);
  });

  it('parses RESILIENT_PUBSUB_MULTIPLIER as a positive float', () => {
    const cfg = resolveConfigFromEnv({ RESILIENT_PUBSUB_MULTIPLIER: '1.5' });
    expect(cfg.multiplier).toBe(1.5);
  });

  it('parses RESILIENT_PUBSUB_STOP_TIMEOUT_MS as a positive integer', () => {
    const cfg = resolveConfigFromEnv({ RESILIENT_PUBSUB_STOP_TIMEOUT_MS: '15000' });
    expect(cfg.stopTimeoutMs).toBe(15000);
  });

  it('parses RESILIENT_PUBSUB_MAX_MESSAGES as a positive integer', () => {
    const cfg = resolveConfigFromEnv({ RESILIENT_PUBSUB_MAX_MESSAGES: '10' });
    expect(cfg.maxMessages).toBe(10);
  });

  it('parses RESILIENT_PUBSUB_MAX_BYTES as a positive integer', () => {
    const cfg = resolveConfigFromEnv({ RESILIENT_PUBSUB_MAX_BYTES: '1048576' });
    expect(cfg.maxBytes).toBe(1048576);
  });

  it('returns all undefined fields when given an empty env object', () => {
    const cfg = resolveConfigFromEnv({});
    expect(cfg.maxAttempts).toBeUndefined();
    expect(cfg.strategy).toBeUndefined();
    expect(cfg.jitter).toBeUndefined();
    expect(cfg.initialDelay).toBeUndefined();
    expect(cfg.maxDelay).toBeUndefined();
    expect(cfg.multiplier).toBeUndefined();
    expect(cfg.stopTimeoutMs).toBeUndefined();
    expect(cfg.maxMessages).toBeUndefined();
    expect(cfg.maxBytes).toBeUndefined();
  });

  it('resolves multiple env vars from the same call simultaneously', () => {
    const cfg = resolveConfigFromEnv({
      RESILIENT_PUBSUB_MAX_ATTEMPTS: '5',
      RESILIENT_PUBSUB_BACKOFF_STRATEGY: 'linear',
      RESILIENT_PUBSUB_JITTER: 'equal',
      RESILIENT_PUBSUB_INITIAL_DELAY: '200',
      RESILIENT_PUBSUB_MAX_MESSAGES: '20',
    });

    expect(cfg.maxAttempts).toBe(5);
    expect(cfg.strategy).toBe('linear');
    expect(cfg.jitter).toBe('equal');
    expect(cfg.initialDelay).toBe(200);
    expect(cfg.maxMessages).toBe(20);
  });
});
