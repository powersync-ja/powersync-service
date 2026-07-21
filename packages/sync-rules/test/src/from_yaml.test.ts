import { describe, expect, test } from 'vitest';
import { LineCounter, parseDocument } from 'yaml';
import { SqlSyncRules } from '../../src/index.js';

describe('SqlSyncRules.fromYaml errors', () => {
  test('invalid edition', () => {
    checkErrors(`
config:
  edition: 'invalid' #error "must be numeric"
bucket_definitions:
  foo:
    data: [SELECT * FROM users]
`);
  });

  test('unknown option', () => {
    checkErrors(`
config:
  unknown_options: true #error "Unknown key 'unknown_options'."
bucket_definitions:
  foo:
    data: [SELECT * FROM users]
`);
  });

  test('time value precision not a string', () => {
    checkErrors(`
config:
  timestamp_max_precision: 3 #error "must be a string"
bucket_definitions:
  foo:
    data: [SELECT * FROM users]
`);
  });

  test('time value precision invalid', () => {
    checkErrors(`
config:
  timestamp_max_precision: years #error "allowed are seconds, milliseconds, microseconds, nanoseconds"
bucket_definitions:
  foo:
    data: [SELECT * FROM users]
`);
  });

  test('missing data', () => {
    checkErrors(`
bucket_definitions:
  foo:
    parameters: SELECT token_parameters.user_id AS user_id #error "Missing required keys: data"
`);
  });

  test('stream query not a scalar', () => {
    checkErrors(`
config:
  edition: 3
streams:
  foo:
    query: [SELECT * FROM users] #error "Expected a scalar here."
`);
  });

  test('stream queries not an array', () => {
    checkErrors(`
config:
  edition: 3
streams:
  foo:
    queries: SELECT * FROM users #error "Expected a sequence here."
`);
  });

  test('priority not a number', () => {
    checkErrors(`
config:
  edition: 3
streams:
  foo:
    priority: sync fast please #error "Invalid priority, expected a number"
    query: SELECT * FROM users
`);
  });

  test('auto_subscribe not a boolean', () => {
    checkErrors(`
config:
  edition: 3
streams:
  foo:
    auto_subscribe: 1 #error "must be a boolean"
    query: SELECT * FROM users
`);
  });
});

interface ExpectedError {
  line: number;
  message: string;
}

const errorCommentPattern = /#error\s+"((?:[^"\\]|\\.)*)"/;

function parseExpectedErrors(source: string): ExpectedError[] {
  return source
    .split('\n')
    .map((lineText, index): ExpectedError | null => {
      const match = errorCommentPattern.exec(lineText);
      if (match == null) {
        return null;
      }

      return { line: index + 1, message: JSON.parse(`"${match[1]}"`) };
    })
    .filter((entry) => entry != null);
}

function checkErrors(yaml: string) {
  const { errors } = SqlSyncRules.fromYaml(yaml, { throwOnError: false, defaultSchema: 'schema' });
  const expectedErrors = parseExpectedErrors(yaml);

  // Use our own LineCounter to translate the character offsets on errors back to line numbers, so that we can match
  // them up against the #error comments (which are only aware of line numbers).
  const lineCounter = new LineCounter();
  parseDocument(yaml, { lineCounter });

  const remainingActualErrors = errors.map((error) => ({
    line: lineCounter.linePos(error.location.start).line,
    message: error.message
  }));

  for (const expected of expectedErrors) {
    const matchIndex = remainingActualErrors.findIndex(
      (actual) => actual.line === expected.line && actual.message.includes(expected.message)
    );

    if (matchIndex == -1) {
      const onSameLine = remainingActualErrors.filter((e) => e.line === expected.line);
      expect.fail(
        `Expected an error on line ${expected.line} matching "${expected.message}", but found: ${
          onSameLine.length > 0
            ? onSameLine.map((e) => JSON.stringify(e.message)).join(', ')
            : '(no errors on this line)'
        }`
      );
    }

    remainingActualErrors.splice(matchIndex, 1);
    errors.splice(matchIndex, 1);
  }

  expect(errors, 'Unexpected errors were reported').toEqual([]);
}
