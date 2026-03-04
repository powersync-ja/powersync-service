import { storage } from '@powersync/service-core';
import {
  ParameterIndexLookupCreator,
  SourceTableInterface,
  SqliteRow,
  TablePattern
} from '@powersync/service-sync-rules';
import { ParameterLookupScope } from '@powersync/service-sync-rules/src/HydrationState.js';
import { bucketRequest } from '../test-utils/general-utils.js';

export function bucketRequestMap(
  syncRules: storage.PersistedSyncRulesContent,
  buckets: Iterable<readonly [string, bigint]>
): storage.BucketDataRequest[] {
  return Array.from(buckets, ([bucketName, opId]) => bucketRequest(syncRules, bucketName, opId));
}

export function bucketRequests(
  syncRules: storage.PersistedSyncRulesContent,
  bucketNames: string[]
): storage.BucketChecksumRequest[] {
  return bucketNames.map((bucketName) => {
    const request = bucketRequest(syncRules, bucketName, 0n);
    return { bucket: request.bucket, source: request.source };
  });
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
