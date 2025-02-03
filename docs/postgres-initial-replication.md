# Postgres: Initial replication

Initial replication is the process of getting existing data from the source database and process it with sync rules, when deploying a sync rules change. After initial replication, we switch to streaming replication.

Once initial replication completed and streaming replication has caught up, the bucket data is in a consistent state and ready to be downloaded.

## Approach 1: Snapshot

This is our first approach.

We start by creating a logical replication slot, exporting a snapshot:

```sql
CREATE_REPLICATION_SLOT < slot > LOGICAL pgoutput EXPORT_SNAPSHOT
```

While that connection stays open, we create another connection with a transaction, and read each table:

```sql
BEGIN;

SET TRANSACTION ISOLATION LEVEL REPEATABLE READ;
SET TRANSACTION READ ONLY;
SET TRANSACTION SNAPSHOT '<snapshot>';
SET LOCAL statement_timeout = 0;

SELECT * FROM <table>;
...

END TRANSACTION;
```

This approach is simple: We get a consistent snapshot from Postgres, that starts at the point we created the replication slot. All data we read is from this snapshot, and we'll read each row exactly once.

### Problems

The issue with this approach is that we cannot resume replication if it fails. There are many reasons the replication can be interrupted:

1. Connection failure.
2. Replication process crashes, for example due to an out-of-memory error.
3. Replication process restarts, for example to a deploy or migration to a different node.
4. We've observed connection timeouts if a query cursor is older than 5 minutes.

In each of those cases, replication has to restart from scratch. In some cases, this can cause repeated failure and restarts, without ever making progress.

Replication has to start from scratch, since the transaction snapshot cannot be persisted. It only lasts as long as there is an open connection using the snapshot.

## Approach 2a: Table snapshot

We already use this approach when we see a new table after initial replication, e.g. if a table containing data is renamed.

1. Create a logical replication slot before querying any tables.
2. Copy each table: `select * from <table>`.
3. Get `pg_current_wal_lsn()`.
4. Wait until streaming replication has reached the LSN from (3).

## Pros and cons

Since this does not depend on a single consistent snapshot, we can resume replication on a per-table basis.

### Consistency

Postgres uses MVCC regardless of isolation level, so the entire query will give one consistent snapshot.

If any rows are updated after we start the query, logical replication will pick up and apply the updates after our snapshot.

If any rows are updated between (1) and (2), then logical replication may revert changes before applying them again.

Say we have this sequence on the source database:

1. Update v = a.
2. Update v = b.
3. `select * from table`.
4. Update v = c;

The sequence of replicated data may be:

1. v = b (`select * from table`)
2. v = a
3. v = b
4. v = c

Waiting until logical replication has caught up ensures this does not cause any consistency issues.

## Approach 2b: Resuming table snapshots

Instead of restarting table replication from scratch when it is interrupted, we can resume replicating the table.

The approach here is to still use `select * from table` to resume, but ignore any rows we've already seen.

### Consistency

When we rerun the same query, it will be at a different point in time from the first, and it may return different results.

If we encounter the same row a second time, the second version may be different from the first. This is safe to ignore: streaming replication will ensure it is updated correctly.

If a row was present in the first query but not the second, it means it was deleted. Streaming replication will cater for this.

If a row was not present in the first query but is present in the second, it is safe to replicate. Streaming replication may re-create the same row which will have some redundancy, but it will not cause any consistency issues.
