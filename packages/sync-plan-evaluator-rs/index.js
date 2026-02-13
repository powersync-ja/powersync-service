import { createRequire } from 'node:module';
import { fileURLToPath } from 'node:url';
import path from 'node:path';

const require = createRequire(import.meta.url);
const dirname = path.dirname(fileURLToPath(import.meta.url));
let native = null;

function getNative() {
  if (native != null) {
    return native;
  }

  native = require(path.join(dirname, 'sync-plan-evaluator-rs.node'));
  return native;
}

function normalizeForRust(value) {
  if (typeof value === 'bigint') {
    const converted = Number(value);
    if (!Number.isSafeInteger(converted)) {
      throw new Error(`BigInt value ${value.toString()} is outside JS safe integer range.`);
    }
    return converted;
  }

  if (value instanceof Uint8Array) {
    return { __bytes: Array.from(value) };
  }

  if (Array.isArray(value)) {
    return value.map(normalizeForRust);
  }

  if (value && typeof value === 'object') {
    return Object.fromEntries(Object.entries(value).map(([key, nested]) => [key, normalizeForRust(nested)]));
  }

  return value;
}

function parseJson(jsonText) {
  return JSON.parse(jsonText);
}

export class RustSyncPlanEvaluator {
  #inner;

  constructor(serializedPlan, options = {}) {
    const asJson =
      typeof serializedPlan === 'string' ? serializedPlan : JSON.stringify(normalizeForRust(serializedPlan));
    this.#inner = new (getNative().NativeSyncPlanEvaluator)(asJson, {
      defaultSchema: options.defaultSchema ?? null
    });
  }

  evaluateRow(options) {
    const payload = JSON.stringify(normalizeForRust(options));
    return parseJson(this.#inner.evaluateRowJson(payload));
  }

  evaluateParameterRow(sourceTable, record) {
    const payload = JSON.stringify(
      normalizeForRust({
        sourceTable,
        record
      })
    );
    return parseJson(this.#inner.evaluateParameterRowJson(payload));
  }

  prepareBucketQueries(options) {
    const payload = JSON.stringify(normalizeForRust(options));
    return parseJson(this.#inner.prepareBucketQueriesJson(payload));
  }

  resolveBucketQueries(prepared, lookupResults) {
    const preparedJson = JSON.stringify(normalizeForRust(prepared));
    const resultsJson = JSON.stringify(normalizeForRust(lookupResults));
    return parseJson(this.#inner.resolveBucketQueriesJson(preparedJson, resultsJson));
  }
}
