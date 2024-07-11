# Compacting Operations

This document describes the internal process of compacting operations. This describes what happens behind the scenes when running the `compact` CLI command.

By default, each change in the source database becomes a new operation in the ever-growing bucket history. To avoid this history from growing indefinitely, we "compact" the buckets.

## Previous Workaround

A workaround is to deploy a sync rules change, which re-creates all buckets from scratch, containing only the latest version of each row. This reduces the size of buckets for new clients performing a sync from scratch, but requires existing clients to completely re-sync the buckets.

# Compacting Processes

The compacting process can be split into three distinct processes.

1. Convert operations into MOVE operations.
2. Convert operations into CLEAR operations.
3. Defragment buckets.

## 1. MOVE operations

Any operation on a row may be converted into a MOVE operation if there is another PUT or REMOVE operation later in the bucket for the same row.

Two rows are considered the same if the combination of `(object_type, object_id, subkey)` is the same.

A MOVE operation may contain data of `{target: '<op_id>'}`. This indicates that the operation was "moved" to the target, and no checksum before that op_id will be valid. This is optional - the server may omit this data if the checkpoint being synced is greater than or equal to the target op_id.

When converting an operation to a MOVE operation, the bucket, op_id and checksum remain the same. The data, object_type, object_id and subkey fields may be cleared, reducing the size of the operation.

By itself, converting operations into MOVE operations does not reduce the number of operations synced, but may reduce the total size of the operations synced. It has no effect on clients that are already up-to-date.

## 2. CLEAR operations

A CLEAR operation in a bucket indicates that all preceeding operations in the bucket must be deleted. It is typically the first operation in a bucket, but the client may receive it at any later point.

The the client has active PUT options before the CLEAR operation, those are effectively converted into REMOVE operations. This will remove the data unless there is another PUT operation for the relevant rows later in the bucket.

The compact process involves:

1. Find the largest sequence of REMOVE, MOVE and/or CLEAR operations of at the start of the bucket.
2. Replace all of those with the single CLEAR operation.

The op_id of the CLEAR operation is the latest op_id of the operations being replaced, and the checksum is the combination of those operations' checksums.

Compacting to CLEAR operations can reduce the number of operations in a bucket. However,
it is not effective if there is a PUT operation near the start of the bucket. This compacting step has no effect on clients that are already up-to-date.

The MOVE compact step above should typically be run before the CLEAR compact step, to ensure maximum effectiveness.

## 3. Defragmentation

Even after doing the MOVE and CLEAR compact processes, there is still a possibility of a bucket being fragmented with many MOVE and REMOVE operations. In the worst case, a bucket may start with a single PUT operation, followed by thousands of MOVE and REMOVE operations. Only a single row (the PUT operation) still exists, but new clients must sync all the MOVE and CLEAR operations.

To handle these cases, we can "defragment" the data.

Defragmentation does not involve any new operations. Instead, it just moves PUT operations for active rows from the start of the bucket to the end of the bucket, to allow the above MOVE and COMPACT processes to efficiently compact the bucket.

The disadvantage here is that these rows will be re-synced by existing clients.

# Implementation

## MOVE + CLEAR

This is a process that compacts all buckets, by iterating through all operations. This process can be run periodically, for example once a day, or after bulk data modifications.

The process iterates through all operations in reverse order. This effectively processes one bucket at a time, in reverse order of operatoins.

We track each row we've seen in a bucket, along with the last PUT/REMOVE operation we've seen for the row. Whenever we see the same row again, we replace that operation with a MOVE operation, using the PUT/REMOVE op_id as the target.

To avoid indefinite memory growth for this process, we place a limit on the memory usage for the set of rows we're tracking. Once we reach this limit, we avoid adding tracking any additional rows for the bucket. We should be able to effectively compact buckets in the order of 10M rows using 1GB of memory, and only lose some compacting gains for larger buckets.

The second part is compacting to CLEAR operations. For each bucket, we keep track of the last PUT operation we've seen (last meaning the smallest op_id since we're iterating in reverse). We then replace all the operations before that with a single CLEAR operation.

## Defragmentation

For an initial workaround, defragmenting can be performed outside powersync by touching all rows in a bucket:

```sql
update mytable set id = id
-- Repeat the above for other tables in the same bucket if relevant
```

After this, the normal MOVE + CLEAR compacting will compact the bucket to only have a single operation per active row.

This would cause existing clients to re-sync every row, while reducing the number of operations for new clients.

If an updated_at column or similar is present, we can use this to defragment more incrementally:

```sql
update mytable set id = id where updated_at < now() - interval '1 week'
```

This version avoids unnecessary defragmentation of rows modified recently.

In the future, we can implement defragmentation inside PowerSync, using heuristics around the spread of operations within a bucket.

# Future additions

Improvements may be implemented in the future:

1. Keeping track of buckets that need compacting would allow us to compact those as soon as needed, without the overhead of compacting buckets where it won't have an effect.
2. Together with the above, we can implement a lightweight compacting process inside the replication worker, to compact operations as soon as modifications come in. This can help to quickly compact in cases where multiple modifications are made to the same rows in a short time span.
3. Implement automatic defragmentation inside PowerSync as described above.
