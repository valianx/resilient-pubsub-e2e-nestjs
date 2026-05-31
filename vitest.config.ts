import { defineConfig } from 'vitest/config';
import swc from 'unplugin-swc';

export default defineConfig({
  // NestJS relies on `emitDecoratorMetadata` so the DI container can read the
  // constructor parameter types behind @Inject(...). Vitest transforms with
  // esbuild, which does NOT emit decorator metadata — without this plugin Nest
  // throws "can't resolve dependencies of the SubscriberService (?)". unplugin-swc
  // runs the SWC transform with decorator metadata enabled, restoring it.
  plugins: [
    swc.vite({
      module: { type: 'es6' },
      jsc: {
        target: 'es2022',
        parser: { syntax: 'typescript', decorators: true },
        transform: { legacyDecorator: true, decoratorMetadata: true },
      },
    }),
  ],
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
