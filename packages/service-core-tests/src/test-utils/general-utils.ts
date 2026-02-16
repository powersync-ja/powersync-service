import { storage, utils } from '@powersync/service-core';
import { GetQuerierOptions, RequestParameters, SqlSyncRules } from '@powersync/service-sync-rules';
import { versionedHydrationState } from '@powersync/service-sync-rules/src/HydrationState.js';
import * as bson from 'bson';

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
    active: true,
    last_checkpoint_lsn: '',
    parsed(options) {
      return {
        id: 1,
        sync_rules: SqlSyncRules.fromYaml(content, options),
        slot_name: 'test',
        hydratedSyncRules() {
          return this.sync_rules.config.hydrate({ hydrationState: versionedHydrationState(1) });
        }
      };
    },
    lock() {
      throw new Error('Not implemented');
    }
  };
}

export function makeTestTable(name: string, replicaIdColumns?: string[] | undefined) {
  const relId = utils.hashData('table', name, (replicaIdColumns ?? ['id']).join(','));
  const id = new bson.ObjectId('6544e3899293153fa7b38331');
  return new storage.SourceTable({
    id: id,
    connectionTag: storage.SourceTable.DEFAULT_TAG,
    objectId: relId,
    schema: 'public',
    name: name,
    replicaIdColumns: (replicaIdColumns ?? ['id']).map((column) => ({ name: column, type: 'VARCHAR', typeId: 25 })),
    snapshotComplete: true
  });
}

export function getBatchData(
  batch: utils.SyncBucketData[] | storage.SyncBucketDataChunk[] | storage.SyncBucketDataChunk
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
  batch: utils.SyncBucketData[] | storage.SyncBucketDataChunk[] | storage.SyncBucketDataChunk
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
  batch: utils.SyncBucketData[] | storage.SyncBucketDataChunk[] | storage.SyncBucketDataChunk
): utils.SyncBucketData | null {
  if (!Array.isArray(batch)) {
    return batch.chunkData;
  }
  if (batch.length == 0) {
    return null;
  }
  let first = batch[0];
  if ((first as storage.SyncBucketDataChunk).chunkData != null) {
    return (first as storage.SyncBucketDataChunk).chunkData;
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

export function querierOptions(globalParameters: RequestParameters): GetQuerierOptions {
  return {
    globalParameters,
    hasDefaultStreams: true,
    streams: {}
  };
}
