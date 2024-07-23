# Formal Bucket Properties

This document describes buckets as a set of operations, along with the properties we guarantee for the sync protocol. These are the properties used to ensure that each client ends up with the same bucket state when the same bucket has been downloaded.

For a more broad overview of the protocol, see [sync-protocol.md](./sync-protocol.md). For a high-level description of the compact implementation, see [compacting-operations.md](./compacting-operations.md).

## Buckets

A bucket $B$ is defined as a set of operations:

```math
B = \{ op_1, op_2, \ldots, op_n \}
```

Each operation $op_i$ is a tuple of:

```math
(opid_i, type_i, rowid_i, data_i, checksum_i)
```

$opid_i \in \mathbb{N}$ is a strictly incrementing series of numbers identifying the operation within the bucket. $type_i$ is one of PUT, REMOVE, MOVE or CLEAR. $rowid_i$ is an identifier uniquely identifying a single row of data within the bucket. $checksum_i$ is a checksum over all the other fields in the tuple.

We define the shorthand syntax for a sub-sequence of bucket $B$ as:

```math
B_{[a..b]} = \{ op_i | a <= opid_i <= b \}
```

```math
B_{[..b]} = B_{[opid_1..b]}
```

## Reducing buckets

We define the function "reduce" on a bucket $B$, $r(B)$. This operation is run client-side, after downloading data for a bucket, to get the final state for each unique row. The reduced state:

1. Discards MOVE operations, only keeping track of their checksum.
2. Discards superseded operations, only keeping the last operation for each unique row (but keeps track of the checksum of prior operations).
3. Discards REMOVE operations (keeping their checkums), only after all prior operations for those rows have been superseded.

In other words, it keeps the lastest state of each row, in addition to the checksum of other discarded operations. We define two reduced buckets as equal if they have exactly the same set of PUT operations, and the remaining checksum is equal.

The algorithm to compute $r(B)$ is as follows:

1. Start with the initial state containing a special CLEAR operation $R = \\{ (0, CLEAR, checksum_t = 0) \\}$.
2. Iterate through the operations $op_i$ in $B$ in sequence, ordered by $opid$.
3. If $type_i = PUT$:
   1. Add $op_i$ to $R$.
   2. Remove any prior operations $op_j$ from $R$ where $rowid_j = rowid_i$ and $opid_j < opid_i$ (always true since we're iterating in sequence).
   3. Add $checksum_j$ to $checksum_t$ for any $op_j$ we removed.
4. If $type_i = REMOVE$:
   1. Remove any prior operations $op_j$ from $R$ where $rowid_j = rowid_i$ and $opid_j < opid_i$ (always true since we're iterating in sequence).
   2. Add $checksum_i$ to $checksum_t$ in the first operation, as well as adding $checksum_j$ for any $op_j$ we removed.
5. If $type_i = MOVE$:
   1. Add $checksum_i$ to $checksum_t$ in the first operation.
6. If $type_i = CLEAR$:
   1. Remove ALL operations from $R$.
   2. Set $R = \\{ (0, CLEAR, checksum_t = checksum_i) \\}$.
7. After iterating through all operations in $B$, return $R$ as the result.

$r(r(B_1) \cup B_2)$ is a common operation takes an already-reduced bucket $r(B_1)$, adds all operations from $B_2$, and reduces it again. This is the action of taking an already-download and reduced on the client bucket, downloading new operations for the bucket, and adding it to the reduced result.

The function $r(B)$ must always satisfy this property:

```math
r(B_{[..opid_n]}) = r(r(B_{[..c_i]}) \cup B_{[c_i+1..opid_n]}) \quad \textrm{for all} \quad c_i \in B
```

That is, a client must have been able to sync to any checkpoint $c_i$, persist the reduced set, then continue syncing the remainder of the data, and end up with the same result as syncing it all in one go. The iterative nature of the algorithm makes the above always true.

## Compacting

We define a server-side compact operation, that reduces the amount of data that needs to be downloaded to clients, while still getting the same results on each client.

When we compact the bucket $B$ to $B' = compact(B)$, the following conditions must hold:

```math
r(B) = r(B')
```

```math
r(B) = r(r(B_{[..c_i]}) \cup B'_{[c_i+1..opid_n]}) \quad \textrm{for all} \quad c_i \in B
```

Note that this is the same as the standard equation for buckets above, but with $B$ replaced by the compacted bucket $B'$ in one place, representing newer data downloaded by clients.

The compact operation is defined in [compacting-operations.md](./compacting-operations.md), a formal description is pending.

The above easily holds when compacting operations into MOVE operations. Since move operations are not present in $r(B)$ apart from their checksum, and the checksum is not changed with $B'$, this has no effect on the outcome.

A proof of compacting into CLEAR operations is pending.

### Consistency during sync

Suppose a client starts downloading at checkpoint $c_1$, operations $B_{[..c_1]}$. Right after, operations are added - checkpoint $c_2$, operations $B_{[c_1+1..c_2]}$. Then, the bucket is compacted to $B'\_{[..c_2]}$. While we have a guaranteed $r(B_{[..c_2]}) = r(B'\_{[..c_2]})$, there is no guarantee that $r(B_{[..c_1]}) = r(B'\_{[..c_1]})$. In fact it's very possible that MOVE operations can cause $r(B_{[..c_1]}) \neq r(B'\_{[..c_1]})$. Therefore, we invalidate the checkpoint $c_1$ - the client will only have consistent data once it has fully synced up to $c_2$.
