import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { parse as parseYaml } from 'yaml';
import { describe, expect, test } from 'vitest';
import {
  CompatibilityContext,
  CompatibilityEdition,
  SqlDataQuery,
  SqlParameterQuery,
  syncStreamFromSql,
  SyncStreamsCompiler
} from '../../../src/index.js';
import { EMPTY_DATA_SOURCE, PARSE_OPTIONS } from '../util.js';
import { grammarAcceptsSql } from './generated_grammar.js';

type FixtureMode = 'bucket_definitions' | 'sync_streams_alpha' | 'new_compiler';
type BucketSlot = 'parameters' | 'data';
type StreamSlot = 'query';
type CompilerSlot = 'query' | 'with';
type FixtureSlot = BucketSlot | StreamSlot | CompilerSlot;

type FixtureKind = 'accepted' | 'rejected_syntax' | 'rejected_semantic';

interface FixtureCase {
  label: string;
  mode: FixtureMode;
  slot: FixtureSlot;
  kind: FixtureKind;
  sql: string;
  parserOk: boolean;
  grammarOk: boolean;
  err?: string;
  params?: string[];
}

interface FixtureEntry {
  sql: string;
  err?: string;
  params?: string[];
}

interface FixtureGroup {
  accepted?: FixtureEntry[];
  rejected_syntax?: FixtureEntry[];
  rejected_semantic?: FixtureEntry[];
}

type FixtureFile = Partial<Record<FixtureSlot, FixtureGroup>>;

interface Outcome {
  accept: boolean;
  messages: string[];
}

interface ListenerError {
  message: string;
  isWarning: boolean;
}

const GRAMMAR_FILE_BY_MODE: Record<FixtureMode, string> = {
  bucket_definitions: fileURLToPath(new URL('../../../grammar/1-bucket-definitions.ebnf', import.meta.url)),
  sync_streams_alpha: fileURLToPath(new URL('../../../grammar/2-sync-streams-alpha.ebnf', import.meta.url)),
  new_compiler: fileURLToPath(new URL('../../../grammar/3-sync-streams-compiler.ebnf', import.meta.url))
};

const fixtures: FixtureCase[] = [
  ...loadFixtureFile('fixtures/bucket_definitions.yaml', 'bucket_definitions'),
  ...loadFixtureFile('fixtures/sync_streams_alpha.yaml', 'sync_streams_alpha'),
  ...loadFixtureFile('fixtures/new_compiler.yaml', 'new_compiler')
];

describe('grammar parity fixtures', () => {
  test.each(fixtures)('parser contract: $mode/$slot/$kind/$label', (fixture) => {
    const outcome = runParser(fixture);
    assertParserExpectation(fixture, outcome);
  });

  test.each(fixtures)('grammar contract: $mode/$slot/$kind/$label', (fixture) => {
    const outcome = runGrammarChecker(fixture);
    assertGrammarExpectation(fixture, outcome);
  });

  test.each(fixtures)('parser/grammar matrix: $mode/$slot/$kind/$label', (fixture) => {
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

function loadFixtureFile(relativePath: string, mode: FixtureMode): FixtureCase[] {
  const filePath = fileURLToPath(new URL(relativePath, import.meta.url));
  const parsed = parseYaml(readFileSync(filePath, 'utf8')) as FixtureFile;
  const output: FixtureCase[] = [];

  for (const [slot, group] of Object.entries(parsed) as Array<[FixtureSlot, FixtureGroup | undefined]>) {
    if (group == null) {
      continue;
    }

    pushGroup(output, mode, slot, 'accepted', group.accepted ?? []);
    pushGroup(output, mode, slot, 'rejected_syntax', group.rejected_syntax ?? []);
    pushGroup(output, mode, slot, 'rejected_semantic', group.rejected_semantic ?? []);
  }

  return output;
}

function pushGroup(
  out: FixtureCase[],
  mode: FixtureMode,
  slot: FixtureSlot,
  kind: FixtureKind,
  entries: FixtureEntry[]
): void {
  entries.forEach((entry, index) => {
    const expected = expectedOutcomes(kind);

    out.push({
      mode,
      slot,
      kind,
      sql: entry.sql,
      err: entry.err,
      params: entry.params,
      parserOk: expected.parserOk,
      grammarOk: expected.grammarOk,
      label: fixtureLabel(entry.sql, index)
    });
  });
}

function expectedOutcomes(kind: FixtureKind): { parserOk: boolean; grammarOk: boolean } {
  switch (kind) {
    case 'accepted':
      return { parserOk: true, grammarOk: true };
    case 'rejected_syntax':
      return { parserOk: false, grammarOk: false };
    case 'rejected_semantic':
      return { parserOk: false, grammarOk: true };
  }
}

function assertParserExpectation(fixture: FixtureCase, outcome: Outcome): void {
  expect(
    {
      mode: fixture.mode,
      slot: fixture.slot,
      kind: fixture.kind,
      sql: fixture.sql,
      expected: fixture.parserOk,
      actual: outcome.accept,
      messages: outcome.messages
    },
    `Parser expectation mismatch for ${fixtureRef(fixture)}`
  ).toMatchObject({ actual: fixture.parserOk });

  if (!fixture.parserOk && fixture.err) {
    const needle = fixture.err.toLowerCase();
    expect(
      outcome.messages.some((message) => message.toLowerCase().includes(needle)),
      `Expected a parser error containing '${fixture.err}' for ${fixtureRef(fixture)}`
    ).toBe(true);
  }
}

function assertGrammarExpectation(fixture: FixtureCase, outcome: Outcome): void {
  expect(
    {
      mode: fixture.mode,
      slot: fixture.slot,
      kind: fixture.kind,
      sql: fixture.sql,
      expected: fixture.grammarOk,
      actual: outcome.accept,
      messages: outcome.messages
    },
    `Grammar expectation mismatch for ${fixtureRef(fixture)}`
  ).toMatchObject({ actual: fixture.grammarOk });
}

function fixtureLabel(sql: string, index: number): string {
  const oneLine = sql.replace(/\s+/g, ' ').trim();
  const clipped = oneLine.length > 60 ? `${oneLine.slice(0, 57)}...` : oneLine;
  return `${index + 1}. ${clipped}`;
}

function fixtureRef(fixture: FixtureCase): string {
  return `${fixture.mode}/${fixture.slot}/${fixture.kind}/${fixture.label}`;
}

function runParser(fixture: FixtureCase): Outcome {
  switch (fixture.mode) {
    case 'bucket_definitions':
      return runBucketParser(fixture);
    case 'sync_streams_alpha':
      return runSyncStreamsAlphaParser(fixture);
    case 'new_compiler':
      return runNewCompilerParser(fixture);
  }
}

function runBucketParser(fixture: FixtureCase): Outcome {
  const slot = fixture.slot as BucketSlot;

  try {
    if (slot === 'parameters') {
      const parsed = SqlParameterQuery.fromSql(
        'bucket',
        fixture.sql,
        {
          defaultSchema: PARSE_OPTIONS.defaultSchema,
          compatibility: PARSE_OPTIONS.compatibility,
          accept_potentially_dangerous_queries: true
        },
        'parity-query',
        EMPTY_DATA_SOURCE
      );

      const messages = hardParserMessages(parsed.errors);
      return { accept: messages.length === 0, messages };
    }

    const parsed = SqlDataQuery.fromSql(
      fixture.params ?? [],
      fixture.sql,
      { defaultSchema: PARSE_OPTIONS.defaultSchema },
      PARSE_OPTIONS.compatibility
    );

    const messages = hardParserMessages(parsed.errors);
    return { accept: messages.length === 0, messages };
  } catch (e) {
    return rejectFromException(e);
  }
}

function runSyncStreamsAlphaParser(fixture: FixtureCase): Outcome {
  try {
    const [_, errors] = syncStreamFromSql('stream', fixture.sql, {
      defaultSchema: PARSE_OPTIONS.defaultSchema,
      compatibility: new CompatibilityContext({ edition: CompatibilityEdition.SYNC_STREAMS }),
      auto_subscribe: false
    });

    const messages = hardParserMessages(errors);
    return { accept: messages.length === 0, messages };
  } catch (e) {
    return rejectFromException(e);
  }
}

function runNewCompilerParser(fixture: FixtureCase): Outcome {
  const compiler = new SyncStreamsCompiler({ defaultSchema: PARSE_OPTIONS.defaultSchema });
  const errors: ListenerError[] = [];

  const listener = {
    report(message: string, _location: unknown, options?: { isWarning: boolean }) {
      errors.push({ message, isWarning: options?.isWarning === true });
    }
  };

  try {
    if (fixture.slot === 'with') {
      const parsed = compiler.commonTableExpression(fixture.sql, listener);
      if (parsed == null && errors.length === 0) {
        errors.push({ message: 'Could not parse CTE SQL.', isWarning: false });
      }
    } else {
      const stream = compiler.stream({ name: 'stream', isSubscribedByDefault: false, priority: 3 });
      stream.addQuery(fixture.sql, listener);
      stream.finish();
    }

    const messages = errors.filter((e) => !e.isWarning).map((e) => e.message);
    return { accept: messages.length === 0, messages };
  } catch (e) {
    return rejectFromException(e);
  }
}

function runGrammarChecker(fixture: FixtureCase): Outcome {
  const grammarPath = GRAMMAR_FILE_BY_MODE[fixture.mode];
  const startRule = grammarStartRule(fixture.mode, fixture.slot);

  const accept = grammarAcceptsSql(grammarPath, startRule, fixture.sql);
  return {
    accept,
    messages: accept ? [] : [`Grammar ${startRule} did not accept SQL.`]
  };
}

function grammarStartRule(mode: FixtureMode, slot: FixtureSlot): string {
  if (mode === 'bucket_definitions') {
    return slot === 'parameters' ? 'ParameterQuery' : 'DataQuery';
  }

  if (mode === 'sync_streams_alpha') {
    return 'SyncStreamsAlphaQuery';
  }

  return slot === 'with' ? 'CompilerCteSubquery' : 'CompilerStreamQuery';
}

function hardParserMessages(errors: Array<{ message: string; type?: string }>): string[] {
  return errors.filter((error) => error.type !== 'warning').map((error) => error.message);
}

function rejectFromException(error: unknown): Outcome {
  if (error instanceof Error) {
    return { accept: false, messages: [error.message] };
  }

  return { accept: false, messages: [String(error)] };
}
