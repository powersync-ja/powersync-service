import { storage } from '@powersync/service-core';
import {
  ParameterIndexLookupCreator,
  ParameterLookupDefinitionId,
  ParameterLookupScope,
  SourceTableRef,
  SqliteRow,
  TablePattern
} from '@powersync/service-sync-rules';
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
  get sourceId(): ParameterLookupDefinitionId {
    return {
      lookupName: 'lookup',
      queryId: '0'
    };
  },
  getSourceTables(): Set<TablePattern> {
    return new Set();
  },
  evaluateParameterRow(_sourceTable: SourceTableRef, _row: SqliteRow) {
    return [];
  },
  tableSyncsParameters(_table: SourceTableRef): boolean {
    return false;
  },

  equals(other) {
    return other === this;
  },
  hash(hasher) {
    hasher.addString(this.sourceId.lookupName);
    hasher.addString(this.sourceId.queryId);
  }
};

export function parameterLookupScope(
  lookupName: string,
  queryId: string,
  source: ParameterIndexLookupCreator = EMPTY_LOOKUP_SOURCE
): ParameterLookupScope {
  return { lookupName, queryId, source };
}
