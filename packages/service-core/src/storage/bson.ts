import * as bson from 'bson';

import { SqliteJsonValue } from '@powersync/service-sync-rules';
import { ReplicaId } from './BucketStorageBatch.js';

type NodeBuffer = Buffer<ArrayBuffer>;

/**
 * Use for internal (bucket storage) data, where we control each field.
 */
export const BSON_DESERIALIZE_INTERNAL_OPTIONS: bson.DeserializeOptions = {
  // use bigint instead of Long
  useBigInt64: true
};

/**
 * Use for data from external sources.
 */
export const BSON_DESERIALIZE_DATA_OPTIONS: bson.DeserializeOptions = {
  // Temporarily disable due to https://jira.mongodb.org/browse/NODE-6764
  useBigInt64: false
};

/**
 * Lookup serialization must be number-agnostic. I.e. normalize numbers, instead of preserving numbers.
 * @param lookup
 */
export const serializeLookupBuffer = (lookup: SqliteJsonValue[]): NodeBuffer => {
  const normalized = lookup.map((value) => {
    if (typeof value == 'number' && Number.isInteger(value)) {
      return BigInt(value);
    } else {
      return value;
    }
  });
  return bson.serialize({ l: normalized }) as NodeBuffer;
};

export const serializeLookup = (lookup: SqliteJsonValue[]) => {
  return new bson.Binary(serializeLookupBuffer(lookup));
};

export const getLookupBucketDefinitionName = (lookup: bson.Binary) => {
  const parsed = bson.deserialize(lookup.buffer, BSON_DESERIALIZE_OPTIONS).l as SqliteJsonValue[];
  return parsed[0] as string;
};

/**
 * True if this is a bson.UUID.
 *
 * Works even with multiple copies of the bson package.
 */
export const isUUID = (value: any): value is bson.UUID => {
  if (value == null || typeof value != 'object') {
    return false;
  }
  const uuid = value as bson.UUID;
  return uuid._bsontype == 'Binary' && uuid.sub_type == bson.Binary.SUBTYPE_UUID;
};

export const serializeReplicaId = (id: ReplicaId): NodeBuffer => {
  return bson.serialize({ id }) as NodeBuffer;
};

export const deserializeReplicaId = (id: Buffer): ReplicaId => {
  const deserialized = deserializeBson(id);
  return deserialized.id;
};

export const deserializeBson = (buffer: Uint8Array): bson.Document => {
  const doc = bson.deserialize(buffer, BSON_DESERIALIZE_DATA_OPTIONS);
  // Temporary workaround due to https://jira.mongodb.org/browse/NODE-6764
  for (let key in doc) {
    const value = doc[key];
    if (value instanceof bson.Long) {
      doc[key] = value.toBigInt();
    }
  }
  return doc;
};

export const serializeBson = (document: any): NodeBuffer => {
  return bson.serialize(document) as NodeBuffer;
};

/**
 * Returns true if two ReplicaId values are the same (serializes to the same BSON value).
 */
export const replicaIdEquals = (a: ReplicaId, b: ReplicaId) => {
  if (a === b) {
    return true;
  } else if (typeof a == 'string' && typeof b == 'string') {
    return a == b;
  } else if (isUUID(a) && isUUID(b)) {
    return a.equals(b);
  } else if (a == null && b == null) {
    return true;
  } else if ((b == null && a != null) || (a == null && b != null)) {
    return false;
  } else {
    // There are many possible primitive values, this covers them all
    return serializeReplicaId(a).equals(serializeReplicaId(b));
  }
};
