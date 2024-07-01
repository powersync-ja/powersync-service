import { describe, test, expect } from 'vitest';

import * as errors from '../../src/errors/errors-index.js';

class CustomJourneyError extends errors.JourneyError {
  constructor() {
    super({
      code: 'CUSTOM_JOURNEY_ERROR',
      description: 'This is a custom error',
      details: 'this is some more detailed information'
    });
  }
}

describe('errors', () => {
  test('it should respond to instanceof checks', () => {
    const error = new CustomJourneyError();

    expect(error instanceof Error).toBe(true);
    expect(error instanceof errors.JourneyError).toBe(true);
    expect(error.name).toBe('CustomJourneyError');
  });

  test('it should serialize properly', () => {
    const error = new CustomJourneyError();

    // The error stack will contain host specific path information. We only care about the header
    // anyway and that the stack is shown - indicated by the initial `at` text
    const initial = `CustomJourneyError: [CUSTOM_JOURNEY_ERROR] This is a custom error
  this is some more detailed information
    at`;

    expect(`${error}`.startsWith(initial)).toBe(true);
  });

  test('utilities should properly match a journey error', () => {
    const standard_error = new Error('non-journey error');
    const error = new CustomJourneyError();

    expect(errors.isJourneyError(standard_error)).toBe(false);
    expect(errors.isJourneyError(error)).toBe(true);

    expect(errors.matchesErrorCode(error, 'CUSTOM_JOURNEY_ERROR')).toBe(true);
    expect(errors.matchesErrorCode(standard_error, 'CUSTOM_JOURNEY_ERROR')).toBe(false);

    expect(errors.getErrorData(error)).toMatchSnapshot();
    expect(errors.getErrorData(standard_error)).toBe(undefined);
  });
});
