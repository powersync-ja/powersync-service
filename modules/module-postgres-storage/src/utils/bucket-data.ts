import { utils } from '@powersync/service-core';
import { models } from '../types/types.js';
import { replicaIdToSubkey } from './bson.js';

export const mapOpEntry = (entry: models.BucketDataDecoded) => {
  if (entry.op == models.OpType.PUT || entry.op == models.OpType.REMOVE) {
    return {
      op_id: utils.timestampToOpId(entry.op_id),
      op: entry.op,
      object_type: entry.table_name ?? undefined,
      object_id: entry.row_id ?? undefined,
      checksum: Number(entry.checksum),
      subkey: replicaIdToSubkey(entry.source_table!, entry.source_key!),
      data: entry.data
    };
  } else {
    // MOVE, CLEAR

    return {
      op_id: utils.timestampToOpId(entry.op_id),
      op: entry.op,
      checksum: Number(entry.checksum)
    };
  }
};
