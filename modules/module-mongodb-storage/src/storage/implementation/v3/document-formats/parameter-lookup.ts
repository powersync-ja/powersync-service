import { deserializeParameterLookup as deserializeParameterLookupCore } from '@powersync/service-core';
import { ScopedParameterLookup, SqliteJsonValue } from '@powersync/service-sync-rules';
import * as bson from 'bson';
import { ParameterIndexId } from '../../BucketDefinitionMapping.js';

export function serializeParameterLookup(lookup: ScopedParameterLookup): bson.Binary {
  return new bson.Binary(bson.serialize({ l: lookup.values.slice(2) }));
}

export function deserializeParameterLookup(lookup: bson.Binary, indexId: ParameterIndexId): SqliteJsonValue[] {
  return [indexId, '', ...deserializeParameterLookupCore(lookup)];
}
