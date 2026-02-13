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

const commentRecord = buildLargeCommentRecord(TARGET_ROW_INPUT_BYTES);
const evaluateRowInput = {
  sourceTable: { connectionTag: 'default', schema: 'test_schema', name: 'comments' },
  record: commentRecord
};
const evaluateRowInputJson = JSONBig.stringify(evaluateRowInput);

const rustEvaluator = new RustSyncPlanEvaluator(serializedPlanJson, { defaultSchema: 'test_schema' });

const evaluateJs = () => evaluateRowWithJsonBig(jsHydrated, evaluateRowInput);
const evaluateSqlite = () => evaluateRowWithJsonBig(sqliteHydrated, evaluateRowInput);
const evaluateRust = () => rustEvaluator.evaluateRowSerialized(evaluateRowInputJson);

console.log(
  `sizes evaluateRow input=${byteLength(evaluateRowInputJson)}B output[js]=${byteLength(evaluateJs())}B output[sqlite]=${byteLength(
    evaluateSqlite()
  )}B output[rust]=${byteLength(evaluateRust())}B`
);

benchmark('evaluateRow js (obj in, JSONBig out)', evaluateJs);
benchmark('evaluateRow sqlite (obj in, JSONBig out)', evaluateSqlite);
benchmark('evaluateRow rust (json in/out)', evaluateRust);

jsEngine.close();
sqliteEngine.close();

function evaluateRowWithJsonBig(hydrated, options) {
  const rows = hydrated.evaluateRow(options);
  return JSONBig.stringify(rows);
}

function benchmark(name, fn) {
  let sink = 0;

  for (let i = 0; i < WARMUP; i++) {
    sink ^= fn().length;
  }

  const started = process.hrtime.bigint();
  for (let i = 0; i < ITERATIONS; i++) {
    sink ^= fn().length;
  }
  const elapsedNs = Number(process.hrtime.bigint() - started);

  const perOpNs = elapsedNs / ITERATIONS;
  const opsPerSecond = (1e9 / perOpNs).toFixed(0);

  console.log(
    `${name.padEnd(30)} ${opsPerSecond.padStart(10)} ops/s (${perOpNs.toFixed(0)} ns/op, ${ITERATIONS} iterations, sink=${sink})`
  );
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
