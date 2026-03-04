import { storage } from '@powersync/service-core';
import {
  BucketDataSource,
  ParameterIndexLookupCreator,
  SourceTableInterface,
  SqliteRow,
  TablePattern
} from '@powersync/service-sync-rules';
import { ParameterLookupScope } from '@powersync/service-sync-rules/src/HydrationState.js';

export function bucketRequest(syncRules: storage.PersistedSyncRulesContent, bucketName: string): string {
  if (/^\d+#/.test(bucketName)) {
    return bucketName;
  }

  const versionedBuckets = storage.STORAGE_VERSION_CONFIG[syncRules.storageVersion]?.versionedBuckets ?? false;
  return versionedBuckets ? `${syncRules.id}#${bucketName}` : bucketName;
}

export function bucketRequestMap(
  syncRules: storage.PersistedSyncRulesContent,
  buckets: Iterable<readonly [string, bigint]>
): storage.BucketDataRequest[] {
  return Array.from(buckets, ([bucketName, opId]) => ({
    bucket: bucketRequest(syncRules, bucketName),
    start: opId,
    source: EMPTY_DATA_SOURCE
  }));
}

export function bucketRequests(
  syncRules: storage.PersistedSyncRulesContent,
  bucketNames: string[]
): storage.BucketChecksumRequest[] {
  return bucketNames.map((bucketName) => ({
    bucket: bucketRequest(syncRules, bucketName),
    source: EMPTY_DATA_SOURCE
  }));
}

const EMPTY_DATA_SOURCE: BucketDataSource = {
  uniqueName: 'empty',
  bucketParameters: [],
  getSourceTables: () => new Set(),
  tableSyncsData: () => false,
  evaluateRow: () => [],
  resolveResultSets: () => {},
  debugWriteOutputTables: () => {}
};

const EMPTY_LOOKUP_SOURCE: ParameterIndexLookupCreator = {
  get defaultLookupScope(): ParameterLookupScope {
    return {
      lookupName: 'lookup',
      queryId: '0',
      source: EMPTY_LOOKUP_SOURCE
    };
  },
  getSourceTables(): Set<TablePattern> {
    return new Set();
  },
  evaluateParameterRow(_sourceTable: SourceTableInterface, _row: SqliteRow) {
    return [];
  },
  tableSyncsParameters(_table: SourceTableInterface): boolean {
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
