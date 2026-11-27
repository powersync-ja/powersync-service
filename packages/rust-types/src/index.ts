import { createRequire } from 'node:module';
import path from 'node:path';
import type { Logger } from 'winston';

const require = createRequire(import.meta.url);

/**
 * Loads the PowerSync Service Node addon from the given `.node` file.
 */
export function loadRustAddon(addonPath: string): typeof RustAddon {
  return require(path.resolve(addonPath)) as typeof RustAddon;
}

/**
 * The amount of numbers describing each document passed to {@link RustAddon.SyncStreamEvaluator.evaluateBsonBlock}.
 */
export const DOCUMENT_WIDTH = 4;

/**
 * Offsets of values describing each row in {@link RustAddon.EvaluatedBlock.rows}.
 *
 * `*Start` and `*End` values are byte offsets into {@link RustAddon.EvaluatedBlock.bytes}.
 */
export const RowColumn = {
  /** The index of the source document. */
  document: 0,
  /** The element index passed to {@link RustAddon.ProcessorGroup.includeData}. */
  element: 1,
  /** The index into the bucket prefixes passed to {@link RustAddon.ProcessorGroup.includeData}. */
  prefix: 2,
  bucketStart: 3,
  bucketEnd: 4,
  tableStart: 5,
  tableEnd: 6,
  idStart: 7,
  idEnd: 8,
  /** Serialized JSON data of the row. */
  dataStart: 9,
  dataEnd: 10,
  /** The checksum of the row, matching `hashData` in `service-core`. */
  checksum: 11,
  WIDTH: 12
} as const;

/**
 * Offsets of values describing each parameter lookup in {@link RustAddon.EvaluatedBlock.parameters}.
 */
export const ParameterColumn = {
  /** The index of the source document. */
  document: 0,
  /** The element index passed to {@link RustAddon.ProcessorGroup.includeParameter}. */
  element: 1,
  /** A BSON document `{l: [lookupName, queryId, ...inputs]}`, matching `serializeLookupBuffer` in `service-core`. */
  lookupStart: 2,
  lookupEnd: 3,
  WIDTH: 4
} as const;

/**
 * Exports of the PowerSync Service Node addon.
 *
 * These are written by hand and need to be kept in sync with the `#[napi]` exports of the addon entrypoint, see
 * `crates/open_edition_js_entrypoint`.
 */
export declare namespace RustAddon {
  function initializeRust(options: RustInitializationOptions): InitializedRustModule;

  interface RustInitializationOptions {
    /**
     * The winston logger to forward Rust log events to.
     */
    logger: Logger;
  }

  /**
   * An opaque handle to the initialized service.
   *
   * We use externals in napi-rs, https://napi.rs/docs/concepts/external
   */
  class InitializedRustModule {
    private constructor();
    #private;
  }

  /**
   * Deserializes a data source serialized with `serializeDataSource`.
   */
  function deserializeStreamDataSource(defaultSchema: string, json: string): StreamDataSource;

  /**
   * Deserializes a parameter index lookup creator serialized with `serializeParameterSource`.
   */
  function deserializeStreamParameterLookupCreator(defaultSchema: string, json: string): StreamParameterLookupCreator;

  /** An opaque handle to a deserialized data source. */
  class StreamDataSource {
    private constructor();
    #private;
  }

  /** An opaque handle to a deserialized parameter index lookup creator. */
  class StreamParameterLookupCreator {
    private constructor();
    #private;
  }

  /**
   * Data sources and parameter index lookup creators ("elements") to evaluate for rows of a source table.
   *
   * The Rust evaluator doesn't match tables against table patterns, so callers must only include elements matching
   * the tables this group is used for. Elements are identified by an index chosen by the caller, which is reported in
   * evaluated outputs.
   *
   * A group can't be changed after it has been used to create a {@link SourceTable}.
   */
  class ProcessorGroup {
    constructor();

    /**
     * Adds a data source. Evaluated rows are emitted once per entry in `prefixes`, see {@link RowColumn.prefix}.
     */
    includeData(element: number, prefixes: string[], source: StreamDataSource): void;

    /**
     * Adds a parameter index lookup creator.
     */
    includeParameter(element: number, lookupName: string, queryId: string, source: StreamParameterLookupCreator): void;
  }

  /**
   * A source table, along with the {@link ProcessorGroup} to evaluate for its rows.
   */
  class SourceTable {
    constructor(connectionTag: string, schema: string, name: string, group: ProcessorGroup);
    #private;
  }

  /**
   * A Sync Streams evaluator, evaluating blocks of source rows into a columnar format.
   */
  class SyncStreamEvaluator {
    constructor(options: SyncStreamEvaluatorOptions);

    /**
     * Evaluates BSON documents.
     *
     * Documents are slices of `buffers`, each described by {@link DOCUMENT_WIDTH} numbers in `documents`: The index
     * into `buffers`, the byte offset and length of the document in that buffer, and the index into `tables`. Buffers
     * are not copied, and must not be modified during the call.
     *
     * Documents are numbered by their position in `documents`.
     */
    evaluateBsonBlock(tables: SourceTable[], buffers: Uint8Array[], documents: Uint32Array): EvaluatedBlock;

    /**
     * Like {@link evaluateBsonBlock}, but evaluates documents on the libuv thread pool.
     *
     * Arguments are validated synchronously, so invalid documents throw instead of rejecting. `tables` and `documents`
     * are copied and may be reused right away, but `buffers` are read without copying and must not be modified until
     * the promise completes. Multiple blocks may be evaluated concurrently.
     */
    evaluateBsonBlockAsync(
      tables: SourceTable[],
      buffers: Uint8Array[],
      documents: Uint32Array
    ): Promise<EvaluatedBlock>;

    /**
     * Evaluates a row given as a JavaScript object, reporting it as document `0`.
     *
     * This is intended for testing, and has not been optimized.
     */
    testEvaluateObject(table: SourceTable, source: Record<string, SqliteValue>): EvaluatedBlock;

    /**
     * Decodes a BSON document using compatibility options configured on this evaluator.
     *
     * Only used to test the Rust BSON decoder.
     */
    testDecodeBson(buffer: Uint8Array): Record<string, null | number | bigint | Uint8Array>;
  }

  type SqliteValue = string | number | bigint | null | Uint8Array;

  /**
   * Outputs of evaluating a block of documents.
   *
   * Text and binary outputs are stored in {@link bytes}, described by `[start, end)` byte ranges in {@link rows} and
   * {@link parameters}. See {@link RowColumn} and {@link ParameterColumn} for the layout of these arrays.
   */
  interface EvaluatedBlock {
    bytes: Buffer;
    rows: Uint32Array;
    parameters: Uint32Array;
    /**
     * Bucket parameters for each parameter lookup in {@link parameters} (the lookup at index `i` in this array is
     * described by `parameters[i * ParameterColumn.WIDTH ...]`).
     *
     * Each lookup has a list of output rows, which are objects of the form `{"0": output0, "1": output1, ...}`.
     */
    bucketParameters: Record<string, SqliteValue>[][];
    errors: EvaluationError[];
  }

  interface EvaluationError {
    document: number;
    /**
     * The element that failed to evaluate, or `undefined` if the document could not be decoded.
     */
    element?: number;
    message: string;
  }

  interface SyncStreamEvaluatorOptions {
    maxTimeValuePrecision?: string;
    timestampsIso8601: boolean;
    fixedJsonExtract: boolean;
    fixedBooleansInJson: boolean;
    /**
     * Rows whose serialized data is larger than this are reported as errors instead of being emitted.
     */
    maxRowSize?: number;
    /**
     * Whether serialized parameter lookups start with the lookup name and query id (`{l: [lookupName, queryId,
     * ...values]}`, like `serializeLookup`). Defaults to `true`.
     *
     * MongoDB storage v3 stores lookups as `{l: values}`, identifying the index separately.
     */
    includeLookupScope?: boolean;
  }
}
