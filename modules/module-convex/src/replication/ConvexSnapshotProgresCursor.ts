import { ReplicationAssertionError } from '@powersync/lib-services-framework';
import bson from 'bson';
import * as t from 'ts-codec';

export const ConvexSnapshotProgressCursor = t.object({
  cursor: t.string.or(t.Null),
  finished: t.boolean
});

export type ConvexSnapshotProgressCursor = t.Encoded<typeof ConvexSnapshotProgressCursor>;

export const DEFAULT_CONVEX_SNAPSHOT_CURSOR: ConvexSnapshotProgressCursor = {
  cursor: null,
  finished: false
};

export const BinaryConvexSnapshotProgressCursor = t.codec(
  'ConvexSnapshotProgressCursor',
  (decoded: ConvexSnapshotProgressCursor) => bson.serialize(ConvexSnapshotProgressCursor.encode(decoded)),
  (encoded) => ConvexSnapshotProgressCursor.decode(bson.deserialize(encoded) as any)
);

/**
 * Decodes an (optional) encoded ConvexSnapshotProgressCursor.
 * @default {DEFAULT_CONVEX_SNAPSHOT_CURSOR}
 * @throws {ReplicationAssertionError} if the value could not be decoded
 */
export function decodeSnapshotProgressCursor(value: Uint8Array | null | undefined): ConvexSnapshotProgressCursor {
  if (value == null) {
    return DEFAULT_CONVEX_SNAPSHOT_CURSOR;
  }

  try {
    return BinaryConvexSnapshotProgressCursor.decode(value);
  } catch (error) {
    throw new ReplicationAssertionError(
      `Convex snapshot progress cursor is not valid JSON: ${error instanceof Error ? error.message : `${error}`}`
    );
  }
}
