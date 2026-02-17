import { BucketDataRequest, InternalOpId, JwtPayload, storage, utils } from '@powersync/service-core';
import {
  GetQuerierOptions,
  RequestParameters,
  SqlSyncRules,
  versionedHydrationState
} from '@powersync/service-sync-rules';
import * as bson from 'bson';

import { SOURCE } from '@powersync/service-sync-rules';

export const ZERO_LSN = '0/0';

export const PARSE_OPTIONS: storage.ParseSyncRulesOptions = {
  defaultSchema: 'public'
};

export const BATCH_OPTIONS: storage.CreateWriterOptions = {
  ...PARSE_OPTIONS,
  zeroLSN: ZERO_LSN,
  storeCurrentData: true
};

export async function resolveTestTable(
  writer: storage.BucketDataWriter,
  name: string,
  replicaIdColumns: string[] | undefined,
  options: { tableIdStrings: boolean },
  idIndex: number = 1
) {
  const relId = utils.hashData('table', name, (replicaIdColumns ?? ['id']).join(','));
  // Semi-hardcoded id for tests, to get consistent output.
  // If the same test uses multiple tables, pass idIndex to get different ids.
  const idString = '6544e3899293153fa7b383' + (30 + idIndex).toString().padStart(2, '0');

  const id = options.tableIdStrings == false ? new bson.ObjectId(idString) : idString;
  let didGenerateId = false;
  const patterns = writer.rowProcessor.getMatchingTablePatterns({
    schema: 'public',
    name: name,
    connectionTag: storage.SourceTable.DEFAULT_TAG
  });
  if (patterns.length == 0) {
    throw new Error(`Table ${name} not found in sync rules`);
  } else if (patterns.length > 1) {
    throw new Error(`Multiple patterns match table ${name} - not supported in test`);
  }
  const pattern = patterns[0];
  const result = await writer.resolveTables({
    connection_id: 1,
    connection_tag: storage.SourceTable.DEFAULT_TAG,

    entity_descriptor: {
      name: name,
      schema: 'public',
      objectId: relId,

      replicaIdColumns: (replicaIdColumns ?? ['id']).map((column) => ({ name: column, type: 'VARCHAR', typeId: 25 }))
    },
    pattern,
    idGenerator: () => {
      if (didGenerateId) {
        throw new Error('idGenerator called multiple times - not supported in tests');
      }
      didGenerateId = true;
      return id;
    }
  });
  const table = result[0];
  if (table == null) {
    throw new Error(`Failed to resolve test table ${name}`);
  }
  return result[0];
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

export function requestParameters(
  jwtPayload: Record<string, any>,
  clientParameters?: Record<string, any>
): RequestParameters {
  return new RequestParameters(new JwtPayload(jwtPayload), clientParameters ?? {});
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
  bucket?: string,
  start?: InternalOpId | string | number
): BucketDataRequest {
  const parsed = isParsedSyncRules(syncRules) ? syncRules : syncRules.parsed(PARSE_OPTIONS);
  const hydrationState = parsed.hydrationState;
  bucket ??= 'global[]';
  const definitionName = bucket.substring(0, bucket.indexOf('['));
  const parameters = bucket.substring(bucket.indexOf('['));
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

/**
 * Removes the source property from an object.
 *
 * This is for tests where we don't care about this value, and it adds a lot of noise in the output.
 */
export function removeSource<T extends { source?: any }>(obj: T): Omit<T, 'source'> {
  const { source, ...rest } = obj;
  return rest;
}

/**
 * Removes the [SOURCE] symbol property from an object.
 *
 * This is for tests where we don't care about this value, and it adds a lot of noise in the output.
 */
export function removeSourceSymbol<T extends { [SOURCE]: any }>(obj: T): Omit<T, typeof SOURCE> {
  const { [SOURCE]: source, ...rest } = obj;
  return rest;
}
