import { Logger } from '@powersync/lib-services-framework';
import { storage, type RustAddon } from '@powersync/service-core';
import { DOCUMENT_WIDTH, ParameterColumn, RowColumn } from '@powersync/service-rust-types';
import { CompatibilityContext, CompatibilityOption, SqliteJsonValue } from '@powersync/service-sync-rules';
import * as bson from 'bson';
import { SerializedParameterLookup } from '../common/PersistedBatch.js';
import { PrecomputedEvaluation } from '../MongoBucketBatch.js';
import { RecordOperation } from '../OperationBatch.js';

/**
 * Whether a row is evaluated in Rust when Rust evaluation is enabled: It must be BSON-encoded, and its table must have
 * a Rust representation (which is not the case for legacy Sync Rules).
 *
 * Rows evaluated in Rust are never evaluated in JavaScript, since the two implementations aren't fully equivalent (e.g.
 * Rust emits `null` for missing columns). If Rust fails to evaluate such a row, it's reported as an evaluation error.
 */
export function isRustEvaluated(op: RecordOperation): boolean {
  const { after, sourceTable } = op.record;
  return (
    op.afterId != null &&
    sourceTable.rustRepresentation != null &&
    after instanceof storage.EncodedSourceRow &&
    after.format == 'bson' &&
    (sourceTable.syncData || sourceTable.syncParameters)
  );
}

/**
 * Evaluates BSON-encoded source rows with the Rust evaluator.
 *
 * Evaluation runs on the libuv thread pool, so callers can start it before loading existing source records and await
 * it afterwards.
 */
export class RustBlockEvaluation {
  private readonly evaluator: RustAddon.SyncStreamEvaluator;

  constructor(
    addon: typeof RustAddon,
    compatibility: CompatibilityContext,
    private readonly logger: Logger
  ) {
    this.evaluator = new addon.SyncStreamEvaluator({
      maxTimeValuePrecision: compatibility.maxTimeValuePrecision?.name,
      timestampsIso8601: compatibility.isEnabled(CompatibilityOption.timestampsIso8601),
      fixedJsonExtract: compatibility.isEnabled(CompatibilityOption.fixedJsonExtract),
      fixedBooleansInJson: compatibility.isEnabled(CompatibilityOption.fixedBooleanInJson),
      // v3 storage identifies the parameter index separately, see serializeParameterLookup.
      includeLookupScope: false
    });
  }

  /**
   * Evaluates operations with BSON-encoded rows, storing outputs in {@link RecordOperation.precomputed}.
   *
   * Only operations matching {@link isRustEvaluated} are evaluated, and all of them get outputs: If a document can't
   * be evaluated (e.g. because Rust can't decode it, or because evaluating the block fails as a whole), its outputs only
   * contain errors.
   *
   * The returned promise never rejects.
   */
  async evaluate(block: readonly RecordOperation[]): Promise<void> {
    const operations: RecordOperation[] = [];
    const tables: RustAddon.SourceTable[] = [];
    const tableIndices = new Map<RustAddon.SourceTable, number>();
    const buffers: Uint8Array[] = [];
    const bufferIndices = new Map<ArrayBufferLike, number>();
    const documents = new Uint32Array(block.length * DOCUMENT_WIDTH);

    let offset = 0;
    for (const op of block) {
      if (op.submittedForEvaluation) {
        continue;
      }

      if (!isRustEvaluated(op)) {
        continue;
      }
      op.submittedForEvaluation = true;
      const rust = op.record.sourceTable.rustRepresentation!;

      let tableIndex = tableIndices.get(rust.table);
      if (tableIndex == null) {
        tableIndex = tables.length;
        tables.push(rust.table);
        tableIndices.set(rust.table, tableIndex);
      }

      // Documents are typically views into a few larger buffers (responses from the source database), so we pass
      // those instead of one buffer per document.
      const bytes = (op.record.after as storage.EncodedSourceRow).bytes;
      let bufferIndex = bufferIndices.get(bytes.buffer);
      if (bufferIndex == null) {
        bufferIndex = buffers.length;
        buffers.push(new Uint8Array(bytes.buffer));
        bufferIndices.set(bytes.buffer, bufferIndex);
      }

      documents[offset++] = bufferIndex;
      documents[offset++] = bytes.byteOffset;
      documents[offset++] = bytes.byteLength;
      documents[offset++] = tableIndex;
      operations.push(op);
    }

    if (operations.length == 0) {
      return Promise.resolve();
    }

    let pending: Promise<RustAddon.EvaluatedBlock>;
    try {
      pending = this.evaluator.evaluateBsonBlockAsync(tables, buffers, documents.subarray(0, offset));
    } catch (e) {
      // Invalid arguments are reported synchronously.
      pending = Promise.reject(e);
    }

    return pending
      .then((block) => this.readOutputs(operations, block))
      .catch((e) => {
        this.logger.error(`Could not evaluate block of ${operations.length} rows in Rust`, e);
        const message = `Could not evaluate row in Rust: ${e instanceof Error ? e.message : String(e)}`;
        for (const op of operations) {
          op.precomputed = failedEvaluation(op, message);
        }
      });
  }

  private readOutputs(operations: RecordOperation[], block: RustAddon.EvaluatedBlock) {
    const { bytes, rows, parameters, bucketParameters } = block;
    const results: PrecomputedEvaluation[] = operations.map(() => ({
      rows: [],
      parameters: [],
      dataErrors: [],
      parameterErrors: []
    }));
    const elementsOf = (document: number) => operations[document].record.sourceTable.rustRepresentation!.group.elements;

    for (const error of block.errors) {
      const result = results[error.document];
      if (result.failure != null) {
        continue;
      }

      if (error.element == null) {
        // The document could not be decoded in Rust (e.g. due to unsupported BSON types). We don't evaluate it in
        // JavaScript, so this is reported as an evaluation error.
        results[error.document] = failedEvaluation(
          operations[error.document],
          `Could not decode row in Rust: ${error.message}`
        );
        continue;
      }

      const element = elementsOf(error.document)[error.element];
      if (element.type == 'data') {
        result.dataErrors.push(error.message);
      } else {
        result.parameterErrors.push(error.message);
      }
    }

    for (let i = 0; i < rows.length; i += RowColumn.WIDTH) {
      const document = rows[i + RowColumn.document];
      const result = results[document];
      if (result.failure != null) {
        continue;
      }

      const element = elementsOf(document)[rows[i + RowColumn.element]];
      if (element.type != 'data') {
        throw new Error('Expected a data source for an evaluated row');
      }

      result.rows.push({
        source: element.scopes[rows[i + RowColumn.prefix]].source,
        bucket: bytes.toString('utf8', rows[i + RowColumn.bucketStart], rows[i + RowColumn.bucketEnd]),
        table: bytes.toString('utf8', rows[i + RowColumn.tableStart], rows[i + RowColumn.tableEnd]),
        id: bytes.toString('utf8', rows[i + RowColumn.idStart], rows[i + RowColumn.idEnd]),
        data: bytes.toString('utf8', rows[i + RowColumn.dataStart], rows[i + RowColumn.dataEnd]),
        checksum: rows[i + RowColumn.checksum]
      });
    }

    for (let i = 0; i < parameters.length; i += ParameterColumn.WIDTH) {
      const document = parameters[i + ParameterColumn.document];
      const result = results[document];
      if (result.failure != null) {
        continue;
      }

      const element = elementsOf(document)[parameters[i + ParameterColumn.element]];
      if (element.type != 'parameter') {
        throw new Error('Expected a parameter lookup source for an evaluated lookup');
      }

      // Copy the lookup, since it outlives this block in source record caches.
      const lookup = Buffer.from(
        bytes.subarray(parameters[i + ParameterColumn.lookupStart], parameters[i + ParameterColumn.lookupEnd])
      );
      // Bucket parameters can't be blobs.
      const outputs = bucketParameters[i / ParameterColumn.WIDTH] as Record<string, SqliteJsonValue>[];
      result.parameters.push(new SerializedParameterLookup(element.source, new bson.Binary(lookup), outputs));
    }

    results.forEach((result, document) => {
      operations[document].precomputed = result;
    });
  }
}

/**
 * Outputs for a row that couldn't be evaluated: It's reported as an evaluation error for each kind of source of its
 * table, and saved without bucket data or parameter lookups.
 */
function failedEvaluation(op: RecordOperation, message: string): PrecomputedEvaluation {
  const { sourceTable } = op.record;
  return {
    rows: [],
    parameters: [],
    dataErrors: sourceTable.syncData ? [message] : [],
    parameterErrors: sourceTable.syncParameters ? [message] : [],
    failure: message
  };
}
