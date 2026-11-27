import { createHash } from 'node:crypto';
import * as sqlite from 'node:sqlite';

import * as bson from 'bson';

import { JSONBig } from '@powersync/service-jsonbig';
import { loadRustAddon, ParameterColumn, RowColumn, type RustAddon } from '@powersync/service-rust-types';
import { resolveRustAddonPath } from '@powersync/service-rust-types/testing';
import { expect, test } from 'vitest';
import { CompatibilityOption } from '../../../../src/compatibility.js';
import {
  BucketDataScope,
  DEFAULT_HYDRATION_STATE,
  EvaluatedParameters,
  EvaluatedRow,
  EvaluationError,
  HydratedSyncConfig,
  HydrateSyncConfigParams,
  nodeSqlite,
  normalizeParameterValue,
  ParameterLookupScope,
  parseSyncConfig,
  RustGroupElement,
  ScopedParameterLookup,
  SourceTableRef,
  SqliteJsonValue,
  SqliteRow,
  SyncConfig,
  UnscopedParameterLookup,
  withBucketSource
} from '../../../../src/index.js';

interface PrepareStreamsOptions {
  allowWarnings?: boolean;
  /**
   * Don't compare evaluation results of the hydrated config with the Rust evaluator.
   *
   * We enable this for tests with known differences.
   */
  skipRustComparison?: boolean;
}

interface SyncTest {
  prepareWithoutHydration(yaml: string, options?: PrepareStreamsOptions): SyncConfig;
  prepareSyncStreams(
    yaml: string,
    params?: HydrateSyncConfigParams,
    options?: PrepareStreamsOptions
  ): HydratedSyncConfig;
}

type Addon = ReturnType<typeof loadRustAddon>;

export const rustModuleTest = test.extend('rust', { scope: 'worker' }, async ({}, { onCleanup }) => {
  const [path, cleanup] = await resolveRustAddonPath();
  onCleanup(cleanup);

  return loadRustAddon(path);
});

export const syncTest = rustModuleTest.extend<{ sync: SyncTest }>({
  sync: async ({ rust }, use) => {
    await use({
      prepareWithoutHydration: (inputs, options) => {
        const { config, errors } = parseSyncConfig(inputs, { defaultSchema: 'test_schema', throwOnError: false });
        if (!options?.allowWarnings) {
          expect(errors).toStrictEqual([]);
        }

        return config;
      },
      prepareSyncStreams(inputs, params?: HydrateSyncConfigParams, options?: PrepareStreamsOptions) {
        const hydrated = this.prepareWithoutHydration(inputs, options).hydrate(
          params ?? { hydrationState: DEFAULT_HYDRATION_STATE, sqlite: nodeSqlite(sqlite) }
        );
        // The Rust evaluator implements the semantics of the JavaScript expression engine.
        const usesSqliteEngine = hydrated.compatibility.isEnabled(CompatibilityOption.sqliteExpressionEngine);
        if (!options?.skipRustComparison && !usesSqliteEngine) {
          compareWithRust(rust, hydrated);
        }
        return hydrated;
      }
    });
  }
});

const DEFAULT_SCHEMA = 'test_schema';

/**
 * The `tbl` table in the default schema used by tests.
 */
export const TEST_TABLE: SourceTableRef = { connectionTag: 'default', schema: DEFAULT_SCHEMA, name: 'tbl' };

/**
 * A Rust evaluator for data sources and parameter index lookup creators of a hydrated sync config, mapping its
 * columnar outputs to the structures used by the JavaScript implementation.
 */
export class RustTestEvaluator {
  readonly evaluator: RustAddon.SyncStreamEvaluator;
  private readonly tables = new Map<string, { table: RustAddon.SourceTable; elements: RustGroupElement[] }>();

  constructor(
    private readonly rust: Addon,
    private readonly config: HydratedSyncConfig
  ) {
    const compatibility = config.compatibility;
    this.evaluator = new rust.SyncStreamEvaluator({
      maxTimeValuePrecision: compatibility.maxTimeValuePrecision?.name,
      timestampsIso8601: compatibility.isEnabled(CompatibilityOption.timestampsIso8601),
      fixedJsonExtract: compatibility.isEnabled(CompatibilityOption.fixedJsonExtract),
      fixedBooleansInJson: compatibility.isEnabled(CompatibilityOption.fixedBooleanInJson)
    });
  }

  /**
   * Returns a Rust source table with a processor group containing all elements matching the table.
   */
  sourceTable(table: SourceTableRef): { table: RustAddon.SourceTable; elements: RustGroupElement[] } {
    const key = JSON.stringify([table.connectionTag, table.schema, table.name]);
    let resolved = this.tables.get(key);
    if (resolved == null) {
      const compiled = this.config.compileToRustGroup(this.rust, table, this.config.getMatchingSources(table))!;

      resolved = {
        table: new this.rust.SourceTable(table.connectionTag, table.schema, table.name, compiled.group),
        elements: compiled.elements
      };
      this.tables.set(key, resolved);
    }
    return resolved;
  }

  /**
   * Evaluates a source row, returning serialized rows (with `data` as a JSON string).
   */
  evaluateSerialized(table: SourceTableRef, record: SqliteRow) {
    const { table: rustTable, elements } = this.sourceTable(table);
    const block = this.evaluator.testEvaluateObject(rustTable, record);
    return { ...decodeBlock(block, elements), elements };
  }

  /**
   * Evaluates a source row, mapping results to the structures used by the JavaScript implementation.
   */
  evaluate(
    table: SourceTableRef,
    record: SqliteRow
  ): {
    rows: EvaluatedRow[];
    parameters: EvaluatedParameters[];
    dataErrors: EvaluationError[];
    parameterErrors: EvaluationError[];
  } {
    const decoded = this.evaluateSerialized(table, record);

    const rows = decoded.rows.map((row): EvaluatedRow => {
      return withBucketSource(
        {
          bucket: row.bucket,
          table: row.table,
          id: row.id,
          data: JSONBig.parse(row.data) as EvaluatedRow['data']
        },
        row.scope.source
      );
    });

    const parameters = decoded.parameters.map(({ lookup, bucketParameters, scope }): EvaluatedParameters => {
      const [lookupName, queryId, ...inputs] = lookup;
      expect([lookupName, queryId]).toStrictEqual([scope.lookupName, scope.queryId]);

      const scoped = ScopedParameterLookup.normalized(scope, UnscopedParameterLookup.normalized(inputs));
      return { lookup: scoped, bucketParameters };
    });

    const dataErrors: EvaluationError[] = [];
    const parameterErrors: EvaluationError[] = [];
    for (const error of decoded.errors) {
      const element = error.element == null ? null : decoded.elements[error.element];
      const mapped = { error: error.message };
      if (element?.type == 'parameter') {
        parameterErrors.push(mapped);
      } else {
        dataErrors.push(mapped);
      }
    }

    return { rows, parameters, dataErrors, parameterErrors };
  }
}

/**
 * Creates a Rust evaluator for data sources and parameter index lookup creators of a hydrated sync config.
 *
 * Returns null if the config contains sources that can't be evaluated in Rust (e.g. legacy Sync Rules).
 */
export function createRustEvaluator(rust: Addon, config: HydratedSyncConfig): RustTestEvaluator | null {
  // Rust doesn't match table patterns, so we compile all sources once to check whether they're supported. The table
  // only filters variants of data sources, so it doesn't matter here.
  const all = {
    bucketDataSources: config.bucketDataSources,
    parameterLookupSources: config.bucketParameterLookupSources
  };
  if (config.compileToRustGroup(rust, TEST_TABLE, all) == null) {
    return null;
  }

  return new RustTestEvaluator(rust, config);
}

/**
 * Decodes the columnar outputs of the Rust evaluator.
 *
 * This also asserts that serialized parameter lookups are byte-for-byte identical to `serializeLookupBuffer` in
 * `service-core`, since storage compares lookups by their serialized form.
 */
function decodeBlock(block: RustAddon.EvaluatedBlock, elements: RustGroupElement[]) {
  const { bytes, rows, parameters, bucketParameters: allBucketParameters } = block;
  const range = (array: Uint32Array, offset: number, start: number) =>
    bytes.toString('utf8', array[offset + start], array[offset + start + 1]);

  const decodedRows: {
    bucket: string;
    table: string;
    id: string;
    data: string;
    checksum: number;
    scope: BucketDataScope;
  }[] = [];
  for (let i = 0; i < rows.length; i += RowColumn.WIDTH) {
    const element = elements[rows[i + RowColumn.element]];
    if (element.type != 'data') {
      throw new Error('Expected row to reference a bucket element');
    }

    const row = {
      bucket: range(rows, i, RowColumn.bucketStart),
      table: range(rows, i, RowColumn.tableStart),
      id: range(rows, i, RowColumn.idStart),
      data: range(rows, i, RowColumn.dataStart),
      checksum: rows[i + RowColumn.checksum],
      scope: element.scopes[rows[i + RowColumn.prefix]]
    };
    // Matches hashData in service-core.
    const expectedChecksum = createHash('sha256')
      .update(`put.${row.table}.${row.id}.${row.data}`)
      .digest()
      .readUInt32LE(0);
    expect(row.checksum).toBe(expectedChecksum);
    decodedRows.push(row);
  }

  const decodedParameters: {
    lookup: SqliteJsonValue[];
    bucketParameters: Record<string, SqliteJsonValue>[];
    scope: ParameterLookupScope;
  }[] = [];
  for (let i = 0; i < parameters.length; i += ParameterColumn.WIDTH) {
    const element = elements[parameters[i + ParameterColumn.element]];
    if (element.type != 'parameter') {
      throw new Error('Expected parameter to reference a parameter element');
    }

    const lookupBytes = bytes.subarray(
      parameters[i + ParameterColumn.lookupStart],
      parameters[i + ParameterColumn.lookupEnd]
    );
    const lookup = bson.deserialize(lookupBytes, { useBigInt64: true }).l as SqliteJsonValue[];
    // Bucket parameters can't be blobs.
    const bucketParameters = allBucketParameters[i / ParameterColumn.WIDTH] as Record<string, SqliteJsonValue>[];

    // Storage compares serialized lookups, so these must match exactly. The JavaScript implementation normalizes
    // integral numbers to bigints, then serializes {l: values}.
    expect(Buffer.from(lookupBytes)).toStrictEqual(
      Buffer.from(bson.serialize({ l: lookup.map(normalizeParameterValue) }))
    );
    decodedParameters.push({ lookup, bucketParameters, scope: element.scope });
  }

  return { rows: decodedRows, parameters: decodedParameters, errors: block.errors };
}

/**
 * Patches row and parameter evaluation on a hydrated config to also evaluate rows with Rust, asserting that both
 * implementations produce the same results.
 *
 * For errors, we only check that either both implementations report errors or neither does, since messages differ.
 */
function compareWithRust(rust: Addon, config: HydratedSyncConfig) {
  let evaluator: RustTestEvaluator | null | undefined;
  const lazyEvaluator = () => {
    if (evaluator === undefined) {
      evaluator = createRustEvaluator(rust, config);
    }
    return evaluator;
  };

  const evaluateRowWithErrors = config.evaluateRowWithErrors.bind(config);
  config.evaluateRowWithErrors = (options) => {
    const result = evaluateRowWithErrors(options);
    const evaluator = lazyEvaluator();

    // The Rust evaluator always evaluates all sources.
    if (evaluator != null && options.bucketDataSources == null) {
      const fromRust = evaluator.evaluate(options.sourceTable, options.record);
      const rustRows = sortRows(fromRust.rows);
      const jsRows = sortRows(result.results);
      expect(rustRows, 'Rust rows should match JavaScript').toStrictEqual(jsRows);
      // The source is not enumerable, so it's not compared by toStrictEqual.
      expect(
        rustRows.every((row, i) => row.source === jsRows[i].source),
        'Rust rows should have the same sources as JavaScript'
      ).toBe(true);
      expectSameErrors(fromRust.dataErrors, result.errors);
    }

    return result;
  };

  const evaluateParameterRowWithErrors = config.evaluateParameterRowWithErrors.bind(config);
  config.evaluateParameterRowWithErrors = (table, row, options) => {
    const result = evaluateParameterRowWithErrors(table, row, options);
    const evaluator = lazyEvaluator();

    if (evaluator != null && options?.parameterLookupSources == null) {
      const fromRust = evaluator.evaluate(table, row);
      expect(sortParameters(fromRust.parameters), 'Rust parameters should match JavaScript').toStrictEqual(
        sortParameters(result.results)
      );
      expectSameErrors(fromRust.parameterErrors, result.errors);
    }

    return result;
  };
}

function expectSameErrors(rust: EvaluationError[], js: EvaluationError[]) {
  expect(
    rust.length > 0,
    `Rust and JavaScript should both report errors or both not. Rust: ${JSON.stringify(rust)}, JavaScript: ${JSON.stringify(js)}`
  ).toBe(js.length > 0);
}

// The order of results across different sources is not specified, so we sort before comparing.

function sortRows(rows: EvaluatedRow[]): EvaluatedRow[] {
  return sortBy(rows, (row) => [row.bucket, row.table, row.id, sortedEntries(row.data)]);
}

function sortParameters(parameters: EvaluatedParameters[]): EvaluatedParameters[] {
  return sortBy(parameters, (p) => [p.lookup.serializedRepresentation, p.bucketParameters.map(sortedEntries)]);
}

function sortedEntries(record: Record<string, SqliteJsonValue>) {
  return Object.entries(record).sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0));
}

function sortBy<T>(values: T[], key: (value: T) => unknown): T[] {
  const withKeys = values.map((value) => ({ value, key: JSONBig.stringify(key(value)) }));
  withKeys.sort((a, b) => (a.key < b.key ? -1 : a.key > b.key ? 1 : 0));
  return withKeys.map((e) => e.value);
}
