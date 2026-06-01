import { storage } from '@powersync/service-core';
import {
  ParameterIndexLookupCreator,
  ParameterLookupDefinitionId,
  ParameterLookupScope,
  SourceTableRef,
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
