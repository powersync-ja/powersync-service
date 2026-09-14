import { ScopedParameterLookup, UnscopedParameterLookup, withBucketSource } from '@powersync/service-sync-rules';
import { ObjectId } from 'bson';
import { Worker } from 'node:worker_threads';
import { replicaIdToSubkey } from '../../../modules/module-mongodb-storage/dist/utils/util.js';
import { parseDocumentId } from '../../../modules/module-mongodb/dist/replication/bufferToSqlite.js';
import { hashData, hashDelete } from '../../service-core/dist/util/utils.js';

const tableId = new ObjectId('66e834cc91d805df11fa0ecb');

/** Additional preparation performed by optimize-replication's MongoRowPreparation.worker.ts. */
export function completePreparation(rows, buffers, wire = false) {
  return rows.map((row, i) => {
    const { id, idBuffer } = parseDocumentId(buffers[i]);
    const subkey = replicaIdToSubkey(tableId, id);
    return {
      ...row,
      replicaIdBson: new Uint8Array(idBuffer.buffer, idBuffer.byteOffset, idBuffer.byteLength),
      subkey,
      deleteChecksum: hashDelete(subkey),
      data: {
        ...row.data,
        results: row.data.results.map((result) => {
          const value = { ...result, checksum: hashData(result.table, result.id, result.data) };
          return wire ? value : withBucketSource(value, result.source);
        })
      }
    };
  });
}

/** One bounded request per worker, matching the original RowPreparationWorker transport. */
export class JsPreparationWorker {
  pending;
  failure;
  closing = false;
  constructor(queries, sqlite, config, table, full) {
    this.config = config;
    this.table = table;
    this.selection = {
      buckets: config.bucketDataSources.map((_, i) => i),
      parameters: config.bucketParameterLookupSources.map((_, i) => i)
    };
    this.worker = new Worker(new URL('./preparation.worker.mjs', import.meta.url), {
      workerData: { queries, sqlite, table, full }
    });
    this.ready = new Promise((resolve, reject) => {
      this.readyResolve = resolve;
      this.readyReject = reject;
    });
    this.worker.on('error', (error) => this.fail(error));
    this.worker.on('exit', (code) => {
      if (!this.closing) this.fail(new Error(`Worker exited: ${code}`));
    });
    this.worker.on('message', (message) => {
      if (message.ready) {
        this.readyResolve();
        return;
      }
      if (message.error) {
        this.fail(new Error(message.error));
        return;
      }
      const pending = this.pending;
      this.pending = undefined;
      pending?.resolve(message.rows);
    });
  }
  fail(error) {
    this.failure ??= error;
    this.readyReject(this.failure);
    this.pending?.reject(this.failure);
    this.pending = undefined;
  }
  async evaluateBsonAsync(buffers) {
    if (this.failure) throw this.failure;
    if (this.pending) throw new Error('Worker request already pending');
    const inputs = buffers.map((raw) => ({
      raw,
      table: this.table,
      tableId: tableId.toHexString(),
      ...this.selection
    }));
    const rows = await new Promise((resolve, reject) => {
      this.pending = { resolve, reject };
      try {
        this.worker.postMessage(inputs);
      } catch (error) {
        this.fail(error);
      }
    });
    if (rows.length !== buffers.length) throw new Error('Worker result count mismatch');
    return rows.map((row) => ({
      ...row,
      data: {
        ...row.data,
        results: row.data.results.map(({ source, ...value }) =>
          withBucketSource(value, this.config.bucketDataSources[source])
        )
      },
      parameters: {
        ...row.parameters,
        results: row.parameters.results.map(({ source, values, bucketParameters }) => ({
          lookup: ScopedParameterLookup.normalized(
            { source: this.config.bucketParameterLookupSources[source], lookupName: values[0], queryId: values[1] },
            new UnscopedParameterLookup(values.slice(2))
          ),
          bucketParameters
        }))
      }
    }));
  }
  async close() {
    this.closing = true;
    this.fail(new Error('Worker disposed'));
    await this.worker.terminate();
  }
}
