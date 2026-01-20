import { BucketDataRequest, InternalOpId, storage, utils } from '@powersync/service-core';
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
          return this.sync_rules.hydrate({ hydrationState: versionedHydrationState(1) });
        },
        hydrationState: versionedHydrationState(1)
      };
    },
    lock() {
      throw new Error('Not implemented');
    }
  };
}

export function makeTestTable(
  name: string,
  replicaIdColumns: string[] | undefined,
  options: { tableIdStrings: boolean }
) {
  const relId = utils.hashData('table', name, (replicaIdColumns ?? ['id']).join(','));
  const id =
    options.tableIdStrings == false ? new bson.ObjectId('6544e3899293153fa7b38331') : '6544e3899293153fa7b38331';
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

export async function resolveTestTable(
  writer: storage.BucketDataWriter,
  name: string,
  replicaIdColumns: string[] | undefined,
  options: { tableIdStrings: boolean },
  idIndex: number = 1
) {
  const relId = utils.hashData('table', name, (replicaIdColumns ?? ['id']).join(','));
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
  const table = result.tables[0];
  if (table == null) {
    throw new Error(`Failed to resolve test table ${name}`);
  }
  return result.tables[0];
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

function isParsedSyncRules(
  syncRules: storage.PersistedSyncRulesContent | storage.PersistedSyncRules
): syncRules is storage.PersistedSyncRules {
  return (syncRules as storage.PersistedSyncRules).sync_rules !== undefined;
}

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
  const source = parsed.sync_rules.bucketDataSources.find((b) => b.uniqueName === definitionName);

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
