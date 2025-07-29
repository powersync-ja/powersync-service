import { beforeAll, expect } from 'vitest';
import { SqlRuleError } from '../src/index.js';

beforeAll(() => {
  expect.extend({
    toBeSqlRuleError(received, expectedMessage, expectedLocation) {
      const { isNot } = this;

      const message = () => {
        return `expected ${received} ${isNot ? ' not' : ''} to be SQL error with ${expectedMessage} at ${expectedLocation}`;
      };

      if (received instanceof SqlRuleError) {
        const actualLocation =
          received.location && received.sql.substring(received.location.start, received.location.end);

        return {
          pass: received.message == expectedMessage && actualLocation == expectedLocation,
          actual: {
            message: received.message,
            location: actualLocation
          },
          message
        };
      } else {
        return {
          pass: false,
          message
        };
      }
    }
  });
});
