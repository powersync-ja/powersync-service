import * as sqlite from 'node:sqlite';
import {
  CompatibilityContext,
  CompatibilityEdition,
  javaScriptExpressionEngine,
  nodeSqliteExpressionEngine,
  PrecompiledSyncConfig,
  RequestParameters,
  RustSyncPlanEvaluator,
  serializeSyncPlan,
  SqlSyncRules,
  versionedHydrationState
} from '../dist/index.js';

const numericArgs = process.argv.slice(2).filter((arg) => /^-?\d+$/.test(arg));
const ITERATIONS = Number.isFinite(Number(numericArgs[0])) ? Number(numericArgs[0]) : 20000;
const WARMUP = Number.isFinite(Number(numericArgs[1])) ? Number(numericArgs[1]) : 2000;

const yaml = `
config:
  edition: 2
  sync_config_compiler: true

streams:
  stream:
    auto_subscribe: true
    query: |
      SELECT c.* FROM comments c
      INNER JOIN issues i ON c.issue_id = i.id
      WHERE i.owner_id = auth.user_id() AND length(c.body) > 3
`;

const parsed = SqlSyncRules.fromYaml(yaml, {
  defaultSchema: 'test_schema',
  throwOnError: true,
  allowNewSyncCompiler: true
});
const plan = parsed.config.plan;
const serializedPlan = serializeSyncPlan(plan);
const compatibility = new CompatibilityContext({ edition: CompatibilityEdition.SYNC_STREAMS });
const sourceTable = { connectionTag: 'default', schema: 'test_schema', name: 'comments' };
const row = { id: 'c1', issue_id: 'i1', body: 'hello world' };
const querierOptions = {
  globalParameters: new RequestParameters(
    { userId: 'user', userIdJson: 'user', parsedPayload: { sub: 'user' }, parameters: {} },
    {}
  ),
  hasDefaultStreams: true,
  streams: {}
};

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

const rustEvaluator = new RustSyncPlanEvaluator(serializedPlan, { defaultSchema: 'test_schema' });

benchmark('evaluateRow javascript', () => {
  jsHydrated.evaluateRow({ sourceTable, record: row });
});

benchmark('evaluateRow sqlite', () => {
  sqliteHydrated.evaluateRow({ sourceTable, record: row });
});

benchmark('evaluateRow rust', () => {
  rustEvaluator.evaluateRow({ sourceTable, record: row });
});

benchmark('prepareBucketQueries javascript', () => {
  jsHydrated.getBucketParameterQuerier(querierOptions);
});

benchmark('prepareBucketQueries sqlite', () => {
  sqliteHydrated.getBucketParameterQuerier(querierOptions);
});

benchmark('prepareBucketQueries rust', () => {
  rustEvaluator.prepareBucketQueries({
    globalParameters: {
      auth: { sub: 'user' },
      connection: {},
      subscription: {}
    },
    hasDefaultStreams: true,
    streams: {}
  });
});

jsEngine.close();
sqliteEngine.close();

function benchmark(name, fn) {
  for (let i = 0; i < WARMUP; i++) {
    fn();
  }

  const started = process.hrtime.bigint();
  for (let i = 0; i < ITERATIONS; i++) {
    fn();
  }
  const elapsedNs = Number(process.hrtime.bigint() - started);

  const perOpNs = elapsedNs / ITERATIONS;
  const opsPerSecond = (1e9 / perOpNs).toFixed(0);

  console.log(
    `${name.padEnd(30)} ${opsPerSecond.padStart(10)} ops/s (${perOpNs.toFixed(0)} ns/op, ${ITERATIONS} iterations)`
  );
}
