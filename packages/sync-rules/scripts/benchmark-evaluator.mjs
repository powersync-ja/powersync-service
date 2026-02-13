import * as sqlite from 'node:sqlite';
import { JSONBig } from '@powersync/service-jsonbig';
import {
  CompatibilityContext,
  CompatibilityEdition,
  javaScriptExpressionEngine,
  nodeSqliteExpressionEngine,
  PrecompiledSyncConfig,
  RustSyncPlanEvaluator,
  serializeSyncPlan,
  SqlSyncRules,
  versionedHydrationState
} from '../dist/index.js';

const numericArgs = process.argv.slice(2).filter((arg) => /^-?\d+$/.test(arg));
const ITERATIONS = Number.isFinite(Number(numericArgs[0])) ? Number(numericArgs[0]) : 2000;
const WARMUP = Number.isFinite(Number(numericArgs[1])) ? Number(numericArgs[1]) : 200;
const RECORD_VARIANTS = Number.isFinite(Number(numericArgs[2])) ? Number(numericArgs[2]) : 256;
const TARGET_ROW_INPUT_BYTES = 100 * 1024;

const yaml = `
config:
  edition: 2
  sync_config_compiler: true

streams:
  stream:
    auto_subscribe: true
    query: |
      SELECT
        c.*,
        lower(c.body) AS body_lower,
        upper(c.body) AS body_upper,
        length(c.body) AS body_len,
        (c.payload_digest || ':' || CAST(length(c.body) AS TEXT)) AS digest_with_len,
        CAST(c.sentiment_score * 100 AS INTEGER) AS sentiment_pct,
        ifnull(c.edited_at, c.created_at) AS last_modified
      FROM comments c
      INNER JOIN issues i ON c.issue_id = i.id
      WHERE i.owner_id = auth.user_id()
        AND c.payload_digest = subscription.parameter('digest')
        AND length(c.body) > 32
`;

const parsed = SqlSyncRules.fromYaml(yaml, {
  defaultSchema: 'test_schema',
  throwOnError: true,
  allowNewSyncCompiler: true
});
const plan = parsed.config.plan;
const serializedPlanJson = JSON.stringify(serializeSyncPlan(plan));
const compatibility = new CompatibilityContext({ edition: CompatibilityEdition.SYNC_STREAMS });

const jsEngine = javaScriptExpressionEngine(compatibility);
const jsHydrated = new PrecompiledSyncConfig(plan, {
  defaultSchema: 'test_schema',
  engine: jsEngine,
  sourceText: ''
}).hydrate({ hydrationState: versionedHydrationState(1) });

const sqliteEngine = nodeSqliteExpressionEngine(sqlite, compatibility);
const sqliteHydrated = new PrecompiledSyncConfig(plan, {
  defaultSchema: 'test_schema',
  engine: sqliteEngine,
  sourceText: ''
}).hydrate({ hydrationState: versionedHydrationState(1) });

const sourceTable = { connectionTag: 'default', schema: 'test_schema', name: 'comments' };
const commentRecord = buildLargeCommentRecord(TARGET_ROW_INPUT_BYTES);
const commentRecordVariants = buildCommentRecordVariants(commentRecord, RECORD_VARIANTS);
const jsInputs = commentRecordVariants.map((record) => ({ sourceTable, record }));
const rustRecordInputsJson = commentRecordVariants.map((record) => JSONBig.stringify(record));
const sourceTableJson = JSONBig.stringify(sourceTable);

const rustEvaluator = new RustSyncPlanEvaluator(serializedPlanJson, { defaultSchema: 'test_schema' });
const rustPreparedSourceTableId = rustEvaluator.prepareEvaluateRowSourceTableSerialized(sourceTableJson);

const evaluateJs = makeRoundRobinEvaluator(jsInputs, (input) => evaluateRowWithJsonBig(jsHydrated, input));
const evaluateSqlite = makeRoundRobinEvaluator(jsInputs, (input) => evaluateRowWithJsonBig(sqliteHydrated, input));
const evaluateRust = makeRoundRobinEvaluator(rustRecordInputsJson, (recordJson) =>
  rustEvaluator.evaluateRowWithPreparedSourceTableSerialized(rustPreparedSourceTableId, recordJson)
);
const rustParseMinimalOnly = makeRoundRobinEvaluator(rustRecordInputsJson, (recordJson) =>
  rustEvaluator.benchmarkParseRecordMinimalSerialized(rustPreparedSourceTableId, recordJson)
);
const rustParseAndSerializeMinimal = makeRoundRobinEvaluator(rustRecordInputsJson, (recordJson) =>
  rustEvaluator.benchmarkParseAndSerializeRecordMinimalSerialized(rustPreparedSourceTableId, recordJson)
);

const sampleJsInputJson = JSONBig.stringify(jsInputs[0]);
const sampleRustRecordInputJson = rustRecordInputsJson[0];
const sampleJsOutput = evaluateRowWithJsonBig(jsHydrated, jsInputs[0]);
const sampleSqliteOutput = evaluateRowWithJsonBig(sqliteHydrated, jsInputs[0]);
const sampleRustOutput = rustEvaluator.evaluateRowWithPreparedSourceTableSerialized(
  rustPreparedSourceTableId,
  rustRecordInputsJson[0]
);
const sampleRustParseMinimalColumns = rustEvaluator.benchmarkParseRecordMinimalSerialized(
  rustPreparedSourceTableId,
  rustRecordInputsJson[0]
);
const sampleRustParseAndSerializeOutput = rustEvaluator.benchmarkParseAndSerializeRecordMinimalSerialized(
  rustPreparedSourceTableId,
  rustRecordInputsJson[0]
);

console.log(
  `sizes evaluateRow js_input=${byteLength(sampleJsInputJson)}B rust_record_input=${byteLength(sampleRustRecordInputJson)}B output[js]=${byteLength(
    sampleJsOutput
  )}B output[sqlite]=${byteLength(sampleSqliteOutput)}B output[rust]=${byteLength(sampleRustOutput)}B parse_min_cols=${sampleRustParseMinimalColumns} parse_min_output=${byteLength(sampleRustParseAndSerializeOutput)}B variants=${commentRecordVariants.length}`
);

benchmark('evaluateRow js (obj in, JSONBig out)', evaluateJs);
benchmark('evaluateRow sqlite (obj in, JSONBig out)', evaluateSqlite);
benchmark('evaluateRow rust (prepared source + record json)', evaluateRust);
benchmark('rust parse-only minimal (record json)', rustParseMinimalOnly);
benchmark('rust parse+serialize minimal (record json)', rustParseAndSerializeMinimal);

jsEngine.close();
sqliteEngine.close();
rustEvaluator.releasePreparedSourceTable(rustPreparedSourceTableId);

function evaluateRowWithJsonBig(hydrated, options) {
  const rows = hydrated.evaluateRow(options);
  return JSONBig.stringify(rows);
}

function benchmark(name, fn) {
  let sink = 0;

  for (let i = 0; i < WARMUP; i++) {
    sink ^= benchmarkValueLength(fn());
  }

  const started = process.hrtime.bigint();
  for (let i = 0; i < ITERATIONS; i++) {
    sink ^= benchmarkValueLength(fn());
  }
  const elapsedNs = Number(process.hrtime.bigint() - started);

  const perOpNs = elapsedNs / ITERATIONS;
  const opsPerSecond = (1e9 / perOpNs).toFixed(0);

  console.log(
    `${name.padEnd(30)} ${opsPerSecond.padStart(10)} ops/s (${perOpNs.toFixed(0)} ns/op, ${ITERATIONS} iterations, sink=${sink})`
  );
}

function benchmarkValueLength(value) {
  if (typeof value == 'string') {
    return value.length;
  }
  if (typeof value == 'number') {
    return value | 0;
  }
  return String(value).length;
}

function makeRoundRobinEvaluator(values, evaluator) {
  let index = 0;
  return () => {
    const value = values[index];
    index++;
    if (index == values.length) {
      index = 0;
    }
    return evaluator(value);
  };
}

function byteLength(value) {
  return Buffer.byteLength(value, 'utf8');
}

function buildLargeCommentRecord(targetBytes) {
  const base = {
    id: 'c1',
    issue_id: 'i1',
    body: joinSentences(3),
    payload_digest: digestFor(1),
    sentiment_score: 0.73,
    created_at: '2026-02-13T10:00:00.000Z',
    edited_at: null,
    language: 'en-US',
    author_id: 'u-42'
  };

  base.reactions = [];
  base.moderation_events = [];
  base.mentions = [];
  base.tags = [];
  base.audit = [];
  base.client = [];
  base.thread_windows = [];

  let idx = 0;
  let serialized = JSONBig.stringify(base);
  while (byteLength(serialized) < targetBytes) {
    for (let i = 0; i < 64; i++) {
      const n = idx + i;
      base.reactions.push({
        like: (n * 7) % 120,
        love: (n * 11) % 70,
        digest: digestFor(n + 1)
      });
      base.moderation_events.push({
        score: Number(((n % 100) / 100).toFixed(2)),
        action: ['create', 'edit', 'reply', 'react'][n % 4],
        at: `2026-02-${String((n % 28) + 1).padStart(2, '0')}T12:00:00.000Z`
      });
      base.mentions.push({
        user: `user_${(n * 13) % 9999}`,
        channel: ['mobile', 'web', 'desktop'][n % 3]
      });
      base.tags.push({
        token: `tok_${n}`,
        keyword: `kw_${n % 48}`,
        topic: ['sync', 'comment', 'priority', 'feedback', 'sdk', 'mobile'][n % 6]
      });
      base.audit.push({
        id: `audit_${n}`,
        actor: `u_${(n * 17) % 1600}`,
        build: `2026.${(n % 24) + 1}.${(n % 10) + 1}`
      });
      base.client.push({
        platform: ['ios', 'android', 'web', 'desktop'][n % 4],
        version: `v${(n % 12) + 1}.${n % 50}.${n % 20}`
      });
      base.thread_windows.push({
        thread: `thread_${Math.floor(n / 4)}`,
        from: `2026-01-${String((n % 28) + 1).padStart(2, '0')}`,
        to: `2026-02-${String((n % 28) + 1).padStart(2, '0')}`
      });
    }
    idx += 64;
    serialized = JSONBig.stringify(base);
  }

  return base;
}

function buildCommentRecordVariants(baseRecord, count) {
  const variants = [];
  for (let i = 0; i < count; i++) {
    variants.push({
      ...baseRecord,
      id: `c${i + 1}`,
      issue_id: `i${(i % 197) + 1}`,
      body: `${baseRecord.body} variant-${i} ${joinSentences(1)}`,
      payload_digest: digestFor(i + 101),
      sentiment_score: Number((((i % 200) - 100) / 100).toFixed(2)),
      created_at: `2026-02-${String((i % 28) + 1).padStart(2, '0')}T${String(i % 24).padStart(2, '0')}:00:00.000Z`,
      edited_at: i % 3 == 0 ? `2026-03-${String((i % 28) + 1).padStart(2, '0')}T09:00:00.000Z` : null
    });
  }
  return variants;
}

function digestFor(index) {
  const base = `digest_${index.toString(16)}_${((index * 2654435761) >>> 0).toString(16)}`;
  return base.padEnd(64, '0').slice(0, 64);
}

function joinSentences(sentences) {
  const corpus = [
    'Sync replication keeps collaborative state consistent across devices.',
    'Users can annotate records and resolve conflicts without downtime.',
    'Comment moderation scores are recalculated when context changes.',
    'The service pipeline batches writes to improve end-to-end latency.',
    'Search indexing runs asynchronously to keep interactive edits smooth.'
  ];

  const parts = [];
  for (let i = 0; i < sentences; i++) {
    parts.push(corpus[i % corpus.length]);
  }
  return parts.join(' ');
}
