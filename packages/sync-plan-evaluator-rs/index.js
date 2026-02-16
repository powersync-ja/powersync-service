import { createRequire } from 'node:module';
import { fileURLToPath } from 'node:url';
import path from 'node:path';
import { JSONBig } from '@powersync/service-jsonbig';

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
  return JSONBig.parse(jsonText);
}

function stringifyForRust(value) {
  return typeof value === 'string' ? value : JSON.stringify(normalizeForRust(value));
}

export class RustSyncPlanEvaluator {
  #inner;

  constructor(serializedPlan, options = {}) {
    const asJson = stringifyForRust(serializedPlan);
    this.#inner = new (getNative().NativeSyncPlanEvaluator)(asJson, {
      defaultSchema: options.defaultSchema ?? null
    });
  }

  evaluateRowSerialized(options) {
    if (typeof options === 'string') {
      return this.#inner.evaluateRowJson(options);
    }

    return this.#inner.evaluateRowObjectJson(options.sourceTable, options.record);
  }

  evaluateRow(options) {
    return parseJson(this.evaluateRowSerialized(options));
  }

  prepareEvaluateRowSourceTableSerialized(sourceTable) {
    if (typeof sourceTable === 'string') {
      return this.#inner.prepareEvaluateRowSourceTableJson(sourceTable);
    }

    return this.#inner.prepareEvaluateRowSourceTableObject(sourceTable);
  }

  prepareEvaluateRowSourceTable(sourceTable) {
    return this.prepareEvaluateRowSourceTableSerialized(sourceTable);
  }

  evaluateRowWithPreparedSourceTableSerialized(preparedSourceTableId, record) {
    if (typeof record === 'string') {
      return this.#inner.evaluateRowWithPreparedSourceTableJson(preparedSourceTableId, record);
    }

    return this.#inner.evaluateRowWithPreparedSourceTableObjectJson(preparedSourceTableId, record);
  }

  evaluateRowWithPreparedSourceTable(preparedSourceTableId, record) {
    return parseJson(this.evaluateRowWithPreparedSourceTableSerialized(preparedSourceTableId, record));
  }

  benchmarkParseRecordMinimalSerialized(preparedSourceTableId, recordJson) {
    return this.#inner.benchmarkParseRecordMinimalJson(preparedSourceTableId, recordJson);
  }

  benchmarkParseAndSerializeRecordMinimalSerialized(preparedSourceTableId, recordJson) {
    return this.#inner.benchmarkParseAndSerializeRecordMinimalJson(preparedSourceTableId, recordJson);
  }

  releasePreparedSourceTable(preparedSourceTableId) {
    return this.#inner.releasePreparedSourceTableJson(preparedSourceTableId);
  }

  evaluateParameterRowSerialized(options) {
    if (typeof options === 'string') {
      return this.#inner.evaluateParameterRowJson(options);
    }

    return this.#inner.evaluateParameterRowObjectJson(options.sourceTable, options.record);
  }

  evaluateParameterRow(sourceTable, record) {
    return parseJson(this.evaluateParameterRowSerialized({ sourceTable, record }));
  }

  prepareBucketQueriesSerialized(optionsJson) {
    return this.#inner.prepareBucketQueriesJson(optionsJson);
  }

  prepareBucketQueries(options) {
    return parseJson(this.prepareBucketQueriesSerialized(stringifyForRust(options)));
  }

  resolveBucketQueriesSerialized(preparedJson, lookupResultsJson) {
    return this.#inner.resolveBucketQueriesJson(preparedJson, lookupResultsJson);
  }

  resolveBucketQueries(prepared, lookupResults) {
    return parseJson(this.resolveBucketQueriesSerialized(stringifyForRust(prepared), stringifyForRust(lookupResults)));
  }
}
