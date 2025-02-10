import * as bson from 'bson';

import { SqliteJsonValue } from '@powersync/service-sync-rules';
import { ReplicaId } from './BucketStorageBatch.js';

type NodeBuffer = Buffer<ArrayBuffer>;

export const BSON_DESERIALIZE_OPTIONS: bson.DeserializeOptions = {
  // use bigint instead of Long
  useBigInt64: true
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

export const deserializeBson = (buffer: Buffer) => {
  return bson.deserialize(buffer, BSON_DESERIALIZE_OPTIONS);
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
