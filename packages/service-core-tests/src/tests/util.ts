import { storage } from '@powersync/service-core';
import {
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

export function bucketRequests(syncRules: storage.PersistedSyncRulesContent, bucketNames: string[]): string[] {
  return bucketNames.map((bucketName) => bucketRequest(syncRules, bucketName));
}

export function bucketRequestMap(
  syncRules: storage.PersistedSyncRulesContent,
  buckets: Iterable<readonly [string, bigint]>
): Map<string, bigint> {
  return new Map(Array.from(buckets, ([bucketName, opId]) => [bucketRequest(syncRules, bucketName), opId]));
}

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
