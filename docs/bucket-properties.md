# Formal Bucket Properties

This document describes buckets as a set of operations, along with the properties we guarantee for the sync protocol. These are the properties used to ensure that each client ends up with the same bucket state when the same bucket has been downloaded.

For a more broad overview of the protocol, see [sync-protocol.md](./sync-protocol.md). For a high-level description of the compact implementation, see [compacting-operations.md](./compacting-operations.md).

## Buckets

A bucket $B$ is defined as a set of operations $B = \{ op_1, op_2, \ldots, op_n \}$. Each operation is a tuple of $(opid_i, type_i, rowid_i, data_i, checksum_i)$. $opid_i \in \N$ is a strictly incrementing series of numbers identifying the operation within the bucket. $type_i$ is one of PUT, REMOVE, MOVE or CLEAR. $rowid_i$ is an identifier uniquely identifying a single row of data within the bucket. $checksum_i$ is a checksum over all the other fields in the tuple.

We define the shorthand syntax for a sub-sequence of bucket $B$ as $B_{[a..b]} = \{ op_i \;|\; a <= id_i <= b \}$, and $B_{[..b]} = B_{[id_1..b]}$.

## Reducing buckets

We define the function "reduce" on a bucket $B$, $r(B)$. This operation is run client-side, after downloading data for a bucket, to get the final state for each unique row. The reduced state:

1. Discards MOVE operations, only keeping track of their checksum.
2. Discards superseded operations, only keeping the last operation for each unique row (but keeps track of the checksum of prior operations).
3. Discards REMOVE operations (keeping their checkums), only after all prior operations for those rows have been superseded.

In other words, it keeps the lastest state of each row, in addition to the checksum of other discarded operations. We define two reduced buckets as equal if they have exactly the same set of PUT operations, and the remaining checksum is equal.

$r(B)$ roughly defined as follows: $r(B) = \{ (0, CLEAR, checksum_t) \} \cup \{ op_i \in B \;|\; type_i = PUT \}$ (TODO: also handle REMOVE and CLEAR operations).

$r(r(B_1) \cup B_2)$ is a common operation takes an already-reduced bucket $r(B_1)$, adds all operations from $B_2$, and reduces it again. This is the action of taking an already-download and reduced on the client bucket, downloading new operations for the bucket, and adding it to the reduced result.

The function $r(B)$ must always satisfy this property:

$r(B_{[..id_n]}) = r(r(B_{[..id_i]}) \cup B_{[id_{i+1}..id_n]}) \;\forall\; i \in [1..n]$

That is, a client must have been able to sync to any point $id_i$, persist the reduced set, then continue syncing the remainder of the data, and end up with the same result as syncing it all in one go.

## Compacting

We define a server-side compact operation, that reduces the amount of data that needs to be downloaded to clients, while keeping that same results.

When we compact the bucket $B$ to $B'$, the following conditions must hold:

1. $r(B) = r(B')$.
2. $r(B_{[..c]}) = r(B'_{[..c]})$ for any valid checkpoint $c$. Note that we can define all $opid$ less than $opid_n$ as invalid checkpoints.
3. $r(B_{[..c]}) = r(r(B_{[..c_i]}) \cup B'_{[c_i+1..c]}) \;\forall\; c_i \in B$.

This easily holds when compacting operations into MOVE operations. Since move operations are not present in $r(B_{[..c]})$ apart from their checksum, and the checksum is not changed, this has zero effect.

TODO: Proof for CLEAR operations.

### Consistency during sync

Suppose a client starts downloading at checkpoint $c_1$, operations $B_{[..c_1]}$. Right after, operations are added - checkpoint $c_2$, operations $B_{[c_1+1..c_2]}$. Right after, the bucket is compacted to $B'_{[..c_2]}$. While we have a guaranteed $r(B_{[..c_2]}) = r(B'_{[..c_2]})$, there is no guarantee that $r(B_{[..c_1]}) = r(B'_{[..c_1]})$. In fact it's very possible that MOVE operations can cause $r(B_{[..c_1]}) \neq r(B'_{[..c_1]})$. Therefore, we invalidate the checkpoint $c_1$ - the client will only have consistent data once it has fully synced up to $c_2$.
