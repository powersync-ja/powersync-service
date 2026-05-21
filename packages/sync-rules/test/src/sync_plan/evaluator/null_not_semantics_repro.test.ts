import { describe, expect, test } from 'vitest';
import {
  CompatibilityContext,
  CompatibilityEdition,
  DEFAULT_HYDRATION_STATE,
  PrecompiledSyncConfig
} from '../../../../src/index.js';
import { javaScriptExpressionEngine } from '../../../../src/sync_plan/engine/javascript.js';
import { compileToSyncPlanWithoutErrors } from '../../compiler/utils.js';
import { TestSourceTable } from '../../util.js';

function prepareSyncStreams(yaml: string) {
  const compatibility = new CompatibilityContext({ edition: CompatibilityEdition.COMPILED_STREAMS });
  const engine = javaScriptExpressionEngine(compatibility);

  const plan = compileToSyncPlanWithoutErrors(yaml);
  return new PrecompiledSyncConfig(plan, compatibility, [], {
    engine,
    sourceText: '',
    defaultSchema: 'test_schema'
  }).hydrate({ hydrationState: DEFAULT_HYDRATION_STATE });
}

describe('SQLite NULL semantics for NOT in sync stream filters', () => {
  const DOCS = new TestSourceTable('docs');

  test('NOT over a NULL scalar expression should not admit the row', () => {
    const desc = prepareSyncStreams(`
config:
  edition: 3

streams:
  stream:
    auto_subscribe: true
    query: SELECT * FROM docs WHERE NOT nullable_flag
`);

    expect(desc.evaluateRow({ sourceTable: DOCS, record: { id: 'null-row', nullable_flag: null } })).toHaveLength(0);
    expect(desc.evaluateRow({ sourceTable: DOCS, record: { id: 'false-row', nullable_flag: 0n } })).toHaveLength(1);
    expect(desc.evaluateRow({ sourceTable: DOCS, record: { id: 'true-row', nullable_flag: 1n } })).toHaveLength(0);
  });

  test('NOT IN over a NULL left operand should not admit the row', () => {
    const desc = prepareSyncStreams(`
config:
  edition: 3

streams:
  stream:
    auto_subscribe: true
    query: SELECT * FROM docs WHERE state NOT IN '["deleted", "archived"]'
`);

    expect(desc.evaluateRow({ sourceTable: DOCS, record: { id: 'null-row', state: null } })).toHaveLength(0);
    expect(desc.evaluateRow({ sourceTable: DOCS, record: { id: 'public-row', state: 'public' } })).toHaveLength(1);
    expect(desc.evaluateRow({ sourceTable: DOCS, record: { id: 'deleted-row', state: 'deleted' } })).toHaveLength(0);
  });

  test('NOT over OR with a NULL operand should not admit the row', () => {
    const desc = prepareSyncStreams(`
config:
  edition: 3

streams:
  stream:
    auto_subscribe: true
    query: SELECT * FROM docs WHERE NOT (nullable_flag OR 0)
`);

    expect(desc.evaluateRow({ sourceTable: DOCS, record: { id: 'null-row', nullable_flag: null } })).toHaveLength(0);
    expect(desc.evaluateRow({ sourceTable: DOCS, record: { id: 'false-row', nullable_flag: 0n } })).toHaveLength(1);
    expect(desc.evaluateRow({ sourceTable: DOCS, record: { id: 'true-row', nullable_flag: 1n } })).toHaveLength(0);
  });
});
