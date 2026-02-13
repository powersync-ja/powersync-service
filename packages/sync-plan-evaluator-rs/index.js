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

  evaluateRowSerialized(optionsJson) {
    return this.#inner.evaluateRowJson(optionsJson);
  }

  evaluateRow(options) {
    return parseJson(this.evaluateRowSerialized(stringifyForRust(options)));
  }

  prepareEvaluateRowSourceTableSerialized(sourceTableJson) {
    return this.#inner.prepareEvaluateRowSourceTableJson(sourceTableJson);
  }

  prepareEvaluateRowSourceTable(sourceTable) {
    return this.prepareEvaluateRowSourceTableSerialized(stringifyForRust(sourceTable));
  }

  evaluateRowWithPreparedSourceTableSerialized(preparedSourceTableId, recordJson) {
    return this.#inner.evaluateRowWithPreparedSourceTableJson(preparedSourceTableId, recordJson);
  }

  evaluateRowWithPreparedSourceTable(preparedSourceTableId, record) {
    return parseJson(
      this.evaluateRowWithPreparedSourceTableSerialized(preparedSourceTableId, stringifyForRust(record))
    );
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

  evaluateParameterRowSerialized(optionsJson) {
    return this.#inner.evaluateParameterRowJson(optionsJson);
  }

  evaluateParameterRow(sourceTable, record) {
    return parseJson(
      this.evaluateParameterRowSerialized(
        stringifyForRust({
          sourceTable,
          record
        })
      )
    );
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
