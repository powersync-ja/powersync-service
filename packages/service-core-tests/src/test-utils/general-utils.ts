import { storage, utils } from '@powersync/service-core';
import { SqlSyncRules } from '@powersync/service-sync-rules';
import * as bson from 'bson';

export interface StorageOptions {
  /**
   * By default, collections are only cleared/
   * Setting this to true will drop the collections completely.
   */
  dropAll?: boolean;

  doNotClear?: boolean;
}
export type StorageFactory = (options?: StorageOptions) => Promise<storage.BucketStorageFactory>;

export const ZERO_LSN = '0/0';

export const PARSE_OPTIONS: storage.ParseSyncRulesOptions = {
  defaultSchema: 'public'
};

export const BATCH_OPTIONS: storage.StartBatchOptions = {
  ...PARSE_OPTIONS,
  zeroLSN: ZERO_LSN,
  storeCurrentData: true
};

export function testRules(content: string): storage.PersistedSyncRulesContent {
  return {
    id: 1,
    sync_rules_content: content,
    slot_name: 'test',
    parsed(options) {
      return {
        id: 1,
        sync_rules: SqlSyncRules.fromYaml(content, options),
        slot_name: 'test'
      };
    },
    lock() {
      throw new Error('Not implemented');
    }
  };
}

export function makeTestTable(name: string, columns?: string[] | undefined) {
  const relId = utils.hashData('table', name, (columns ?? ['id']).join(','));
  const id = new bson.ObjectId('6544e3899293153fa7b38331');
  return new storage.SourceTable(
    id,
    storage.SourceTable.DEFAULT_TAG,
    relId,
    'public',
    name,
    (columns ?? ['id']).map((column) => ({ name: column, type: 'VARCHAR', typeId: 25 })),
    true
  );
}

export function getBatchData(
  batch: utils.SyncBucketData[] | storage.SyncBucketDataBatch[] | storage.SyncBucketDataBatch
) {
  const first = getFirst(batch);
  if (first == null) {
    return [];
  }
  return first.data.map((d) => {
    return {
      op_id: d.op_id,
      op: d.op,
      object_id: d.object_id,
      checksum: d.checksum
    };
  });
}

export function getBatchMeta(
  batch: utils.SyncBucketData[] | storage.SyncBucketDataBatch[] | storage.SyncBucketDataBatch
) {
  const first = getFirst(batch);
  if (first == null) {
    return null;
  }
  return {
    has_more: first.has_more,
    after: first.after,
    next_after: first.next_after
  };
}

function getFirst(
  batch: utils.SyncBucketData[] | storage.SyncBucketDataBatch[] | storage.SyncBucketDataBatch
): utils.SyncBucketData | null {
  if (!Array.isArray(batch)) {
    return batch.batch;
  }
  if (batch.length == 0) {
    return null;
  }
  let first = batch[0];
  if ((first as storage.SyncBucketDataBatch).batch != null) {
    return (first as storage.SyncBucketDataBatch).batch;
  } else {
    return first as utils.SyncBucketData;
  }
}

/**
 * Replica id in the old Postgres format, for backwards-compatible tests.
 */
export function rid(id: string): bson.UUID {
  return utils.getUuidReplicaIdentityBson({ id: id }, [{ name: 'id', type: 'VARCHAR', typeId: 25 }]);
}
