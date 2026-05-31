import { defineConfig } from 'vitest/config';

export default defineConfig({
  test: {
    // Each suite gets its own isolated worker process to prevent
    // shared state between Nest TestingModule instances.
    pool: 'forks',
    testTimeout: 30_000,
    hookTimeout: 30_000,
    include: ['test/**/*.e2e.test.ts'],
    // Sequential execution avoids emulator port contention across suites.
    sequence: {
      concurrent: false,
    },
  },
});
