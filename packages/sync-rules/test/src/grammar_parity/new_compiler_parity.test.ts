import { describe, expect, test } from 'vitest';
import {
  assertGrammarExpectation,
  assertParserExpectation,
  fixtureRef,
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

  test.each(fixtures)('parser/grammar matrix: $slot/$kind/$label', (fixture) => {
    const parserOutcome = runParser(fixture);
    const grammarOutcome = runGrammarChecker(fixture);

    expect(
      {
        parser: parserOutcome.accept,
        grammar: grammarOutcome.accept
      },
      `Parser/grammar matrix mismatch for ${fixtureRef(fixture)}`
    ).toEqual({
      parser: fixture.parserOk,
      grammar: fixture.grammarOk
    });
  });
});
