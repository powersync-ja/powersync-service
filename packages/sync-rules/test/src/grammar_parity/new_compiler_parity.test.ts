import { describe, test } from 'vitest';
import {
  assertGrammarExpectation,
  assertParserExpectation,
  loadFixtureFile,
  runGrammarChecker,
  runParser
} from './parity_helpers.js';

const fixtures = loadFixtureFile('fixtures/new_compiler.yaml', 'new_compiler');

describe('grammar parity fixtures: new_compiler', () => {
  test.each(fixtures)('parser contract: $slot/$kind/$label', (fixture) => {
    const outcome = runParser(fixture);
    assertParserExpectation(fixture, outcome);
  });

  test.each(fixtures)('grammar contract: $slot/$kind/$label', (fixture) => {
    const outcome = runGrammarChecker(fixture);
    assertGrammarExpectation(fixture, outcome);
  });
});
