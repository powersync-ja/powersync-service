import 'vitest';

interface CustomMatchers<R = unknown> {
  toBeSqlRuleError: (message: string, location: string) => R;
}

declare module 'vitest' {
  interface Assertion<T = any> extends CustomMatchers<T> {}
  interface AsymmetricMatchersContaining extends CustomMatchers {}
}
