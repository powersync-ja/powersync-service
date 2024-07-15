# Sync Protocol

This document is a work in progress - more details to be added over time.

# Sync stream

A sync stream contains 5 possible messages:

## StreamingSyncCheckpoint

Format:

```ts
{
  checkpoint: {
    last_op_id: OpId;
    write_checkpoint?: OpId;
    buckets: BucketChecksum[];
  }
}
```

This indicates a new checkpoint is available, along with checksums for each bucket in the checkpoint.

This is typically (but not necessarily) be the first message in the response, and is often followed by StreamingSyncData and StreamingSyncCheckpointComplete (both optional).

## StreamingSyncCheckpointDiff

This has the same conceptual meaning as a StreamingSyncCheckpoint. It is an optimization to only sent details for buckets that changed, instead of sending the entire checkpoint over.

Format:

```ts
{
  checkpoint_diff: {
    last_op_id: OpId;
    write_checkpoint?: OpId;
    updated_buckets: BucketChecksum[];
    removed_buckets: string[];
  }
}
```

Typically a sync stream would contain a single StreamingSyncCheckpoint, and then after that only StreamingSyncCheckpointDiff messages. However, the server may send any number of StreamingSyncCheckpoint messags after that, each time "resetting" the checkpoint state.

## StreamingSyncData

This message contains data for a single bucket.

Format:

```ts
{
  data: SyncBucketData;
}
```

While this data is typically associated with the last checkpoint sent, it may be sent at any other time, and may contain data outside the last checkpoint boundaries.

## StreamingSyncCheckpointComplete

This message indicates that all data has been sent for the last checkpoint.

Format:

```ts
{
  checkpoint_complete: {
    last_op_id: OpId;
  }
}
```

`last_op_id` here _must_ match the `last_op_id` of the last `StreamingSyncCheckpoint` or `StreamingSyncCheckpointDiff` message sent.

It is not required to have one `StreamingSyncCheckpointComplete` message for every `StreamingSyncCheckpoint` or `StreamingSyncCheckpointDiff`. In some cases, a checkpoint may be invalidated while data were being sent, in which case no matching `StreamingSyncCheckpointComplete` will be sent. Instead, another `StreamingSyncCheckpoint` or `StreamingSyncCheckpointDiff` will be sent with a new checkpoint.

## StreamingSyncKeepalive

Serves as both a general keepalive message to keep the stream open, and an indication of how long before the current token expires.

Format:

```ts
{
  token_expires_in: number;
}
```

# Bucket operations

## PUT

Includes contents for an entire row.

The row is uniquely identified using the combination of (object_type, object_id, subkey).

Another PUT or REMOVE operation with the same `(object_type, object_id, subkey)` _replaces_
this operation.

Another PUT or REMOVE operation with the same `(object_type, object_id, subkey)` but a different `subkey` _does not_ directly replace this one, and may exist concurrently.

Format:

```ts
{
  op_id: OpId;
  op: 'PUT';
  object_type: string;
  object_id: string;
  data: string;
  checksum: number;
  subkey?: string;
}
```

## REMOVE

Indicates that a row is removed from a bucket.

The row is uniquely identified using the combination of (object_type, object_id, subkey).

Another PUT or REMOVE operation with the same `(object_type, object_id, subkey)` _replaces_
this operation.

Format:

```ts
{
  op_id: OpId;
  op: 'REMOVE';
  object_type: string;
  object_id: string;
  checksum: number;
  subkey?: string;
}
```

## MOVE

Indicates a "tombstone" operation - an operation that has been replaced by another one.
The checksum of the operation is unchanged.

Format:

```ts
{
  op_id: OpId;
  op: 'MOVE';
  checksum: number;
}
```

## CLEAR

Indicates a "reset point" of a bucket. All operations prior to this bucket must be removed.

If the client has any active PUT operations prior to a CLEAR operation, those should effectively be converted into REMOVE operations.

The checksum of this operation is equal to the combined checksum of all prior operations it replaced.

Format:

```ts
{
  op_id: OpId;
  op: 'CLEAR';
  checksum: number;
}
```
