import { storage } from '@powersync/service-core';
import {
  ParameterIndexLookupCreator,
  ParameterLookupDefinitionId,
  ParameterLookupScope,
  SourceTableRef,
  TablePattern
} from '@powersync/service-sync-rules';
import { bucketRequest } from '../test-utils/general-utils.js';

/**
 * Resolve storage again after test writes activate the sync config. The storage
 * instance used by the writer retains its original PROCESSING stream snapshot,
 * while compact() expects an instance constructed with the current state.
 */
export async function compactActive(factory: storage.BucketStorageFactory, options: storage.CompactOptions) {
  const active = await factory.getActiveSyncConfig();
  if (active == null) {
    throw new Error('Expected an active sync config before compacting');
  }
  await active.storage.compact(options);
}

export function bucketRequestMap(
  syncRules: storage.PersistedSyncConfigContent,
  buckets: Iterable<readonly [string, bigint]>
): storage.BucketDataRequest[] {
  return Array.from(buckets, ([bucketName, opId]) => bucketRequest(syncRules, bucketName, opId));
}

export function bucketRequests(
  syncRules: storage.PersistedSyncConfigContent,
  bucketNames: string[]
): storage.BucketChecksumRequest[] {
  return bucketNames.map((bucketName) => {
    const request = bucketRequest(syncRules, bucketName, 0n);
    return { bucket: request.bucket, source: request.source };
  });
}

const EMPTY_LOOKUP_SOURCE: ParameterIndexLookupCreator = {
  get sourceId(): ParameterLookupDefinitionId {
    return {
      lookupName: 'lookup',
      queryId: '0'
    };
  },
  getSourceTables(): Set<TablePattern> {
    return new Set();
  },
  createEvaluator(input) {
    return {
      evaluateParameterRow(sourceTable, row) {
        return [];
      }
    };
  },
  tableSyncsParameters(_table: SourceTableRef): boolean {
    return false;
  }
};

export function parameterLookupScope(
  lookupName: string,
  queryId: string,
  source: ParameterIndexLookupCreator = EMPTY_LOOKUP_SOURCE
): ParameterLookupScope {
  return { lookupName, queryId, source };
}
