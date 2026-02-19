import { describe, test } from 'vitest';
import {
  assertGrammarExpectation,
  assertMatrixExpectation,
  assertParserExpectation,
  loadFixtureFile,
  runGrammarChecker,
  runParser
} from './parity_helpers.js';

const fixtures = loadFixtureFile('fixtures/bucket_definitions.yaml', 'bucket_definitions');

describe('grammar parity fixtures: bucket_definitions', () => {
  test.each(fixtures)('parser contract: $slot/$kind/$label', (fixture) => {
    const outcome = runParser(fixture);
    assertParserExpectation(fixture, outcome);
  });

  test.each(fixtures)('grammar contract: $slot/$kind/$label', (fixture) => {
    const outcome = runGrammarChecker(fixture);
    assertGrammarExpectation(fixture, outcome);
  });

  test.each(fixtures)('parser/grammar matrix: $slot/$kind/$label', (fixture) => {
    const parserOutcome = runParser(fixture);
    const grammarOutcome = runGrammarChecker(fixture);
    assertMatrixExpectation(fixture, parserOutcome, grammarOutcome);
  });
});
