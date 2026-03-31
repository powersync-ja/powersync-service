import * as bson from 'bson';
import { deserializeParameterLookup } from '@powersync/service-core';
import { ScopedParameterLookup, SqliteJsonValue } from '@powersync/service-sync-rules';
import { ParameterIndexId } from '../BucketDefinitionMapping.js';

export function serializeParameterLookupV3(lookup: ScopedParameterLookup): bson.Binary {
  return new bson.Binary(bson.serialize({ l: lookup.values.slice(2) }));
}

export function deserializeParameterLookupV3(lookup: bson.Binary, indexId: ParameterIndexId): SqliteJsonValue[] {
  return [indexId, '', ...deserializeParameterLookup(lookup)];
}
