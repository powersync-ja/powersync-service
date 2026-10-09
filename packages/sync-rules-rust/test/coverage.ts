import { createRequire } from 'node:module';
import { afterAll } from 'vitest';

if (process.env.LLVM_PROFILE_FILE) {
  const { flushCoverage } = createRequire(import.meta.url)('../dist/evaluator.node');
  if (!flushCoverage) throw new Error('Native coverage requires an instrumented addon');
  afterAll(() => flushCoverage());
}
