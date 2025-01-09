import { describe, test, expect } from 'vitest';

import * as errors from '../../src/errors/errors-index.js';

class CustomServiceError extends errors.ServiceError {
  constructor() {
    super({
      code: 'PSYNC_S9999',
      description: 'This is a custom error',
      details: 'this is some more detailed information'
    });
  }
}

describe('errors', () => {
  test('it should respond to instanceof checks', () => {
    const error = new CustomServiceError();

    expect(error instanceof Error).toBe(true);
    expect(error instanceof errors.ServiceError).toBe(true);
    expect(error.name).toBe('CustomServiceError');
  });

  test('it should serialize properly', () => {
    const error = new CustomServiceError();

    // The error stack will contain host specific path information. We only care about the header
    // anyway and that the stack is shown - indicated by the initial `at` text
    const initial = `CustomServiceError: [PSYNC_S9999] This is a custom error
  this is some more detailed information
    at`;

    expect(`${error}`.startsWith(initial)).toBe(true);
  });

  test('utilities should properly match a service error', () => {
    const standard_error = new Error('non-service error');
    const error = new CustomServiceError();

    expect(errors.isServiceError(standard_error)).toBe(false);
    expect(errors.isServiceError(error)).toBe(true);

    expect(errors.matchesErrorCode(error, 'PSYNC_S9999')).toBe(true);
    expect(errors.matchesErrorCode(standard_error, 'PSYNC_S9999')).toBe(false);

    expect(errors.getErrorData(error)).toMatchSnapshot();
    expect(errors.getErrorData(standard_error)).toBe(undefined);
  });
});
