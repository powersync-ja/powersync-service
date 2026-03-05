import { BucketDataRequest, InternalOpId, storage, utils } from '@powersync/service-core';
import { GetQuerierOptions, RequestParameters } from '@powersync/service-sync-rules';
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

export function makeTestTable(
  name: string,
  replicaIdColumns?: string[] | undefined,
  options?: { tableIdStrings: boolean }
) {
  const relId = utils.hashData('table', name, (replicaIdColumns ?? ['id']).join(','));
  const id =
    options?.tableIdStrings == false ? new bson.ObjectId('6544e3899293153fa7b38331') : '6544e3899293153fa7b38331';
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

function isParsedSyncRules(
  syncRules: storage.PersistedSyncRulesContent | storage.PersistedSyncRules
): syncRules is storage.PersistedSyncRules {
  return (syncRules as storage.PersistedSyncRules).sync_rules !== undefined;
}

/**
 * Bucket names no longer purely depend on the sync rules.
 * This converts a bucket name like "global[]" into the actual bucket name, for use in tests.
 */
export function bucketRequest(
  syncRules: storage.PersistedSyncRulesContent | storage.PersistedSyncRules,
  bucket: string,
  start?: InternalOpId | string | number
): BucketDataRequest {
  const parsed = isParsedSyncRules(syncRules) ? syncRules : syncRules.parsed(PARSE_OPTIONS);
  const hydrationState = parsed.hydrationState;
  const parameterStart = bucket.indexOf('[');
  const definitionName = bucket.substring(0, parameterStart);
  const parameters = bucket.substring(parameterStart);
  const source = parsed.sync_rules.config.bucketDataSources.find((b) => b.uniqueName === definitionName);

  if (source == null) {
    throw new Error(`Failed to find global bucket ${bucket}`);
  }
  const bucketName = hydrationState.getBucketSourceScope(source).bucketPrefix + parameters;
  return {
    bucket: bucketName,
    start: BigInt(start ?? 0n),
    source: source
  };
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
