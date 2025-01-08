import { storage, utils } from '@powersync/service-core';
import * as uuid from 'uuid';

/**
 * BSON is used to serialize certain documents for storage in BYTEA columns.
 * JSONB columns do not directly support storing binary data which could be required in future.
 */

export function replicaIdToSubkey(tableId: string, id: storage.ReplicaId): string {
  // Hashed UUID from the table and id
  if (storage.isUUID(id)) {
    // Special case for UUID for backwards-compatiblity
    return `${tableId}/${id.toHexString()}`;
  }
  const repr = storage.serializeBson({ table: tableId, id });
  return uuid.v5(repr, utils.ID_NAMESPACE);
}
