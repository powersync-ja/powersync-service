import { deserializeParameterLookup } from '@powersync/service-core';
import { ParameterIndexId, ScopedParameterLookup, SqliteJsonValue } from '@powersync/service-sync-rules';
import * as bson from 'bson';

export function serializeParameterLookupV3(lookup: ScopedParameterLookup): bson.Binary {
  return new bson.Binary(bson.serialize({ l: lookup.values.slice(2) }));
}

export function deserializeParameterLookupV3(lookup: bson.Binary, indexId: ParameterIndexId): SqliteJsonValue[] {
  return [indexId, '', ...deserializeParameterLookup(lookup)];
}
