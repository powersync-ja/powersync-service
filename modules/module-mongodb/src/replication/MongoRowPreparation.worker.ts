import { storage, utils } from '@powersync/service-core';
import { JSONBig } from '@powersync/service-jsonbig';
import { HydratedSyncConfig, nodeSqlite } from '@powersync/service-sync-rules';
import { ObjectId } from 'bson';
import * as sqlite from 'node:sqlite';
import { parentPort, workerData } from 'node:worker_threads';
import { bufferToSqlite, getDateRenderMode, parseDocumentId } from './bufferToSqlite.js';

const config = HydratedSyncConfig.fromWorkerData(workerData, nodeSqlite(sqlite));
const dateMode = getDateRenderMode(config.compatibility);
// Stable source arrays allow the hydrated evaluator to reuse its compiled selections.
const selections = new Map<
  string,
  {
    buckets: typeof config.bucketDataSources;
    parameters: typeof config.bucketParameterLookupSources;
  }
>();

parentPort!.on('message', (inputs: storage.RowPreparationInput[]) => {
  const output = inputs.map((input): storage.RowPreparationOutput => {
    const key = JSON.stringify([input.buckets, input.parameters]);
    let selection = selections.get(key);
    if (selection == null) {
      selection = {
        buckets: input.buckets.map((i) => config.bucketDataSources[i]),
        parameters: input.parameters.map((i) => config.bucketParameterLookupSources[i])
      };
      selections.set(key, selection);
    }
    const buffer = Buffer.from(input.raw.buffer, input.raw.byteOffset, input.raw.byteLength);
    const row = bufferToSqlite(buffer, dateMode);
    const { id: replicaId, idBuffer } = parseDocumentId(buffer);
    const subkey = utils.mongoReplicaIdToSubkey(new ObjectId(input.tableId), replicaId);
    const data = config.evaluateRowWithErrors({
      record: row,
      sourceTable: input.table,
      bucketDataSources: selection.buckets
    });
    const parameters = config.evaluateParameterRowWithErrors(input.table, row, {
      parameterLookupSources: selection.parameters
    });
    return {
      replicaIdBson: idBuffer,
      subkey,
      deleteChecksum: utils.hashDelete(subkey),
      id: typeof row.id === 'object' ? null : row.id,
      data: {
        errors: data.errors.map(({ error }) => ({ error })),
        results: data.results.map((value) => {
          const json = JSONBig.stringify(value.data);
          return {
            bucket: value.bucket,
            table: value.table,
            id: value.id,
            source: config.bucketDataSources.indexOf(value.source),
            json,
            checksum: utils.hashData(value.table, value.id, json)
          };
        })
      },
      parameters: {
        errors: parameters.errors.map(({ error }) => ({ error })),
        results: parameters.results.map((value) => ({
          source: config.bucketParameterLookupSources.indexOf(value.lookup.source),
          values: value.lookup.values,
          bucketParameters: value.bucketParameters
        }))
      }
    };
  });
  parentPort!.postMessage(output);
});
