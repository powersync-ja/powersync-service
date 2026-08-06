# MSSQL Schema Change Flows

This page defines how SQL Server schema changes should be applied when PowerSync uses Change Data Capture (CDC).
It describes the capture-instance-pinning design and compares it with the implementation on `main` before pinning was
introduced.

## Design boundary

An MSSQL replication stream fixes two things when it starts:

1. The exact set of source tables selected by the sync config.
2. The CDC capture-table `object_id` used for each selected source table.

The capture-table identity, rather than its name, is persisted with the PowerSync `SourceTable`. A restart restores the
same identity. A newer capture instance does not change the binding of an existing stream.

This boundary is necessary because SQL Server does not put table and capture-instance DDL into the ordered CDC row
stream. PowerSync discovers those changes by polling database metadata, and that poll cannot be made atomic with a CDC
commit. Automatically changing the meaning of a running stream after such a poll could expose a checkpoint for a state
that never existed in the source database.

In this document, **deploy a new stream** means deploying or redeploying the sync config so that PowerSync creates new
replication state and performs any required snapshots. The sync config text may be unchanged when the only change is a
new CDC capture instance.

## Schema change matrix

The final column references shared risk groups described in [Grouped potential issues in old `main`](#grouped-potential-issues-in-old-main).
Multiple groups can apply to one schema change. A referenced risk was possible under the old control flow; it does not
imply that every occurrence of the corresponding DDL produced an inconsistency.

| Schema change or condition                                                                                           | Effect in SQL Server / CDC                                                                                                                                                                    | Behavior of the pinned PowerSync stream                                                                                                                         | Flow for applying the change                                                                                                                                                                                                     | Behavior in the old `main` implementation                                                                                                                                                                                                                                                     | Potential issues in the old implementation                                                                                                                                                                                                                  |
| -------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Add a table to replication                                                                                           | A new source table and CDC capture instance are created.                                                                                                                                      | The running stream ignores it because its table set is fixed. Table wildcards are not supported.                                                                | Create the table, enable CDC, add the exact table name to the sync config, and deploy a new stream. The new stream snapshots the table before activation.                                                                        | A missing exact table or a wildcard match could be discovered later by the metadata poller and snapshotted inline.                                                                                                                                                                            | [Delayed table admission](#delayed-table-admission).                                                                                                                                                                                                        |
| Configured table does not exist                                                                                      | No source or capture-table identity is available to bind.                                                                                                                                     | Stream startup fails with `PSYNC_S1602`. A not-yet-bound processing stream may retry after the table becomes ready.                                             | Prefer creating the table and enabling CDC before deploying the config. If deployment happened first, make the table ready and allow processing to retry.                                                                        | Startup created no `SourceTable` or per-table checkpoint barrier for the missing table and could complete the global snapshot without it.                                                                                                                                                     | [Delayed table admission](#delayed-table-admission).                                                                                                                                                                                                        |
| Configured table exists but CDC is not enabled                                                                       | Rows cannot be read incrementally.                                                                                                                                                            | Stream startup fails with `PSYNC_S1602`.                                                                                                                        | Enable CDC before deploying the sync config. If this is a new, not-yet-bound stream, enable CDC and let it retry.                                                                                                                | Startup skipped the table, continued replicating other tables, and automatically added and snapshotted the table when CDC was enabled.                                                                                                                                                        | [Delayed table admission](#delayed-table-admission).                                                                                                                                                                                                        |
| Create a second capture instance while the pinned instance remains                                                   | SQL Server exposes two CDC change tables for the same source table. They may capture different column sets.                                                                                   | The stream warns and continues polling the capture-table `object_id` to which it is pinned.                                                                     | Deploy a new stream to adopt the newest capture instance. Keep the pinned instance until the new stream has completed its snapshots and is active, then remove the old instance.                                                 | A live schema poll selected the newest instance, dropped the existing PowerSync table mapping, and re-snapshotted it. On startup, PowerSync also selected the newest instance. It only forced a limited re-snapshot when the persisted resume LSN was older than that instance's minimum LSN. | [Automatic capture replacement](#automatic-capture-replacement).                                                                                                                                                                                            |
| Add, drop, or change a non-identity column without refreshing CDC                                                    | Existing capture instances keep their captured column list. SQL Server records pending DDL for the instance.                                                                                  | The stream warns about schema drift and continues with its pinned captured schema. The DDL change is not adopted by that stream.                                | If the changed column does not need to be replicated, no PowerSync action is required. To replicate the new row shape, create a second capture instance with the desired columns and use the second-capture-instance flow above. | The stream also warned and continued with the existing capture instance until another instance appeared.                                                                                                                                                                                      | [No distinct issue](#no-distinct-issue). If a replacement is created, [automatic capture replacement](#automatic-capture-replacement).                                                                                                                      |
| Change captured columns while a rolling second capture instance is possible                                          | The old capture instance retains the old row shape; the new instance captures the new shape.                                                                                                  | The old stream remains available on the old shape and warns about the newer instance.                                                                           | Apply the allowed DDL, create the second instance, deploy a new stream, wait for it to snapshot and become active, and only then remove the old instance. Update explicit sync queries as needed.                                | The metadata poller adopted the new instance and initiated an inline drop and re-snapshot.                                                                                                                                                                                                    | [Automatic capture replacement](#automatic-capture-replacement).                                                                                                                                                                                            |
| Change a primary key, rename an identity column, or make another change that requires disabling CDC                  | SQL Server requires the existing capture instance to be removed. Re-enabling CDC creates a new capture table with a new `object_id`; the replica identity or captured schema may also differ. | Removing the pinned instance stops the stream with `PSYNC_S1601`. It never adopts the replacement.                                                              | Disable CDC, apply the DDL, re-enable CDC, update the sync config if required, and deploy a new stream. The new stream validates the new identity and snapshots the table.                                                       | Losing the instance disabled replication only for that table while the job continued. Re-enabling CDC caused the replacement to be adopted and the table to be dropped and re-snapshotted inline.                                                                                             | [Lost capture history](#lost-capture-history) and [automatic capture replacement](#automatic-capture-replacement).                                                                                                                                          |
| Drop the pinned capture instance, with or without a replacement                                                      | The change table containing the stream's unread history is permanently removed. A replacement has a different `object_id`, even if it has the same definition.                                | The stream stops with `PSYNC_S1601`. This stream is terminal because its pinned history cannot be restored.                                                     | Ensure a suitable capture instance exists, then deploy a new stream. The config content may be unchanged, but a fresh snapshot is required.                                                                                      | The table was disabled in the in-memory cache while the rest of the replication job continued. If a replacement appeared, it was automatically adopted and the table was re-snapshotted.                                                                                                      | [Lost capture history](#lost-capture-history); with a replacement, also [automatic capture replacement](#automatic-capture-replacement).                                                                                                                    |
| Drop a replicated table                                                                                              | Dropping the source table also removes its capture instances and may destroy unread CDC history.                                                                                              | The stream stops with `PSYNC_S1603`. Existing replicated data is retained until a new config resolves the removal.                                              | Prefer removing the table from the sync config, deploying and activating the new stream, and then dropping the source table. If the source table is dropped first, deploy a new stream to remove it from replicated state.       | The metadata poller automatically dropped the PowerSync table mapping and requested a checkpoint. If the service was stopped during the drop, an exact table pattern could be skipped on restart and stale data could remain.                                                                 | [Table removal ahead of replication lag](#table-removal-ahead-of-replication-lag) and [table disappearance while stopped](#table-disappearance-while-stopped).                                                                                              |
| Rename a replicated table                                                                                            | The source table normally keeps its `object_id` and capture instance, but its name no longer matches the configured exact source.                                                             | The stream stops with `PSYNC_S1603` and retains existing replicated data. It does not guess whether the rename should remain in or move out of the sync config. | Update the sync config to select the new qualified name, or remove the old source from the config, then deploy a new stream. The new stream resolves the operator's intent and snapshots or removes data as required.            | The poller dropped the old mapping and, if the new name matched a pattern, automatically resolved and snapshotted the renamed table. Wildcards could make the renamed table move into or out of scope automatically.                                                                          | If the new name no longer matched, [table removal ahead of replication lag](#table-removal-ahead-of-replication-lag); if an exact configured table was renamed away while stopped, [table disappearance while stopped](#table-disappearance-while-stopped). |
| Drop and recreate a table with the same qualified name                                                               | The recreated source table and its CDC capture table have new `object_id` values. It is a different source identity even though the name is reused.                                           | The old stream stops when its original table disappears. It does not bind the replacement.                                                                      | Create and CDC-enable the replacement, validate its schema and replica identity, and deploy a new stream. The fresh stream removes the old mapping and snapshots the replacement.                                                | The poller treated this as an automatic drop followed by a create, removing the old mapping and snapshotting the replacement.                                                                                                                                                                 | [Collapsed drop and recreate](#collapsed-drop-and-recreate), [lost capture history](#lost-capture-history), and [delayed table admission](#delayed-table-admission).                                                                                        |
| Create a table not selected by an exact table name                                                                   | No exact configured source refers to the table. A wildcard in an old sync config could still match it.                                                                                        | The stream ignores it.                                                                                                                                          | No PowerSync action is required. Add the exact table name and deploy a new sync config only if the table should enter replication.                                                                                               | Exact patterns ignored it. A matching wildcard intentionally admitted it after the metadata poller detected it.                                                                                                                                                                               | If a wildcard selected the table, [delayed table admission](#delayed-table-admission).                                                                                                                                                                      |
| Change indexes, constraints, defaults, or other metadata without changing the captured row shape or replica identity | CDC row decoding remains compatible. Future DML may reflect new defaults or constraints normally.                                                                                             | The pinned stream can continue. It may warn if SQL Server reports the DDL as pending schema drift.                                                              | No new stream is required unless the change alters data selected by the sync config or the operator wants a refreshed capture schema. Update existing rows explicitly if their values must change.                               | Usually warning-only or no action.                                                                                                                                                                                                                                                            | [No distinct issue](#no-distinct-issue).                                                                                                                                                                                                                    |

## Grouped potential issues in old `main`

### Delayed table admission

Old `main` could activate a sync config without creating a `SourceTable` or checkpoint barrier for an exact table that
did not exist or was not CDC-enabled. It could also admit a newly wildcard-matching table later. Wildcard matching was
intentional; the issue was that a metadata poll detected and snapshotted the table only after other changes could
already have been committed.

Until that happened, checkpoints could expose changes from already tracked tables, including relational data referring
to rows in the missing table. The later snapshot converged final row state, but clients could temporarily observe only
one side of the relationship. See the old [startup table discovery][main-table-discovery],
[global snapshot finalization][main-initial-snapshot], [later table discovery][main-new-table-discovery], and
[inline snapshot handler][main-inline-table-snapshot].

### Automatic capture replacement

When a newer capture instance appeared, old `main` selected it, dropped the PowerSync table mapping, and re-snapshotted
the table inline. That live path converged final row state, but the timing and snapshot cost were controlled by metadata
polling rather than a deployment.

The correctness risk was most pronounced across a restart: startup also selected the newest capture instance and only
forced a limited re-snapshot when the persisted resume LSN was older than that instance's minimum LSN. If the resume LSN
had already advanced beyond that minimum, PowerSync could reuse snapshot state produced with the old instance while
streaming the new captured shape. [Issue #730](https://github.com/powersync-ja/powersync-service/issues/730) reproduces
this race. See the old [replacement detection][main-capture-detection],
[automatic re-snapshot][main-new-capture-handler], and [restart snapshot check][main-restart-status].

### Lost capture history

When an existing capture instance disappeared, old `main` immediately cleared it from the table cache. The next poll
skipped that table but could still commit the batch end LSN for the whole stream. If replication lag was present, this
could advance client-visible checkpoints past unread changes from the deleted capture table and expose inconsistent
cross-table state. The CDC drop itself was not emitted as a source event.

If replication was caught up, that specific pre-drop gap did not occur, although changes after CDC was disabled were
not captured. A later replacement and snapshot could converge final row state, but could not restore the missing CDC
events. See the old [missing-instance detection][main-capture-detection],
[missing-instance handler][main-missing-capture-handler], and [poll-and-commit loop][main-poll-commit].

### Table removal ahead of replication lag

This applied when old `main` observed a table drop while running, or observed a rename whose new name no longer matched
the sync config. Schema changes were handled before the next CDC poll, and the handler removed all replicated data for
the table immediately.

Pending deletes for the removed table were subsumed by that removal, so its final empty state still converged. However,
related deletes or updates for other tables could still be in the replication lag. A client-visible checkpoint could
therefore show the removed table as empty while related rows from the same source transition were still present. Later
checkpoints would converge after the remaining lag was processed. See the old
[schema-check-before-poll loop][main-schema-before-poll], [drop handler][main-drop-handler],
[rename handler][main-rename-handler], and [poll-and-commit loop][main-poll-commit].

### Table disappearance while stopped

This applied when an exact configured table was dropped or renamed away while PowerSync was stopped. Startup only
resolved physical tables that currently matched the sync config, so it did not emit the live drop or rename event that
would have removed the old PowerSync mapping. Previously replicated data could therefore remain until the sync config
was redeployed. See the old [startup table discovery][main-table-discovery],
[drop detection][main-drop-detection], and [rename detection][main-rename-detection].

### Collapsed drop and recreate

If a table was dropped and recreated with the same qualified name between metadata checks, old `main` could observe the
new physical table and the disappearance of the old one in the same poll. New-table detection ran first, so the poller
enqueued the replacement create before the old-table drop even though the source DDL occurred in the opposite order.
Neither observation had an ordered CDC position. The replacement was then automatically resolved and snapshotted,
potentially adopting a different replica identity or captured shape. See the old
[new-table-before-drop detection][main-new-table-discovery], [drop detection][main-drop-detection], and
[inline snapshot handler][main-inline-table-snapshot].

### No distinct issue

Continuing with an existing capture instance after compatible DDL was warning-only and did not introduce one of the
old automatic-adoption issues above. Existing stored rows were not rewritten by DDL, which is expected. If the operator
created or recreated a capture instance, the automatic-capture-replacement group applies. See the old
[column-drift detection][main-column-drift-detection] and [schema-drift handler][main-column-drift-handler].

[main-table-discovery]: https://github.com/powersync-ja/powersync-service/blob/main/modules/module-mssql/src/replication/CDCStream.ts#L179-L243
[main-initial-snapshot]: https://github.com/powersync-ja/powersync-service/blob/main/modules/module-mssql/src/replication/CDCStream.ts#L473-L539
[main-new-table-discovery]: https://github.com/powersync-ja/powersync-service/blob/main/modules/module-mssql/src/replication/CDCPoller.ts#L332-L444
[main-inline-table-snapshot]: https://github.com/powersync-ja/powersync-service/blob/main/modules/module-mssql/src/replication/CDCStream.ts#L782-L804
[main-capture-detection]: https://github.com/powersync-ja/powersync-service/blob/main/modules/module-mssql/src/replication/CDCPoller.ts#L362-L382
[main-schema-before-poll]: https://github.com/powersync-ja/powersync-service/blob/main/modules/module-mssql/src/replication/CDCPoller.ts#L128-L151
[main-poll-commit]: https://github.com/powersync-ja/powersync-service/blob/main/modules/module-mssql/src/replication/CDCPoller.ts#L193-L243
[main-new-capture-handler]: https://github.com/powersync-ja/powersync-service/blob/main/modules/module-mssql/src/replication/CDCStream.ts#L751-L759
[main-restart-status]: https://github.com/powersync-ja/powersync-service/blob/main/modules/module-mssql/src/replication/CDCStream.ts#L571-L619
[main-column-drift-detection]: https://github.com/powersync-ja/powersync-service/blob/main/modules/module-mssql/src/replication/CDCPoller.ts#L407-L413
[main-column-drift-handler]: https://github.com/powersync-ja/powersync-service/blob/main/modules/module-mssql/src/replication/CDCStream.ts#L807-L830
[main-missing-capture-handler]: https://github.com/powersync-ja/powersync-service/blob/main/modules/module-mssql/src/replication/CDCStream.ts#L764-L770
[main-drop-detection]: https://github.com/powersync-ja/powersync-service/blob/main/modules/module-mssql/src/replication/CDCPoller.ts#L351-L359
[main-drop-handler]: https://github.com/powersync-ja/powersync-service/blob/main/modules/module-mssql/src/replication/CDCStream.ts#L760-L763
[main-rename-detection]: https://github.com/powersync-ja/powersync-service/blob/main/modules/module-mssql/src/replication/CDCPoller.ts#L385-L404
[main-rename-handler]: https://github.com/powersync-ja/powersync-service/blob/main/modules/module-mssql/src/replication/CDCStream.ts#L725-L743

## Error and warning outcomes

| Outcome                      | Meaning                                                                          | Required action                                                                    |
| ---------------------------- | -------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------- |
| Schema-drift warning         | The source table changed, but the pinned capture schema is still readable.       | Continue on the old shape or deploy a new stream with an updated capture instance. |
| New-capture-instance warning | A newer instance exists while the pinned instance is still available.            | Keep both instances until a newly deployed stream is active.                       |
| `PSYNC_S1601`                | The pinned capture-table `object_id` is gone.                                    | Create or select the desired capture instance and deploy a new stream.             |
| `PSYNC_S1602`                | A table required by a not-yet-bound stream does not exist or is not CDC-enabled. | Create the table and enable CDC; the processing stream can then retry.             |
| `PSYNC_S1603`                | A bound source table was dropped or renamed.                                     | Deploy a config that explicitly removes it or selects its new qualified name.      |
| `PSYNC_R2201`                | The MSSQL sync config uses a table or schema wildcard.                           | Replace the wildcard with exact table names.                                       |

## Why a deployment is the recovery boundary

PowerSync could technically recover some cases inside the existing stream by dropping the old mapping, snapshotting the
table, setting a `no_checkpoint_before` position, and waiting for the replacement capture instance to pass that
position. That can converge final row state, but it does not recover intermediate events from a period where CDC was
unavailable. It can also adopt an unintended schema and trigger expensive snapshot work without operator approval.

The pinning design therefore uses `no_checkpoint_before` for snapshots that belong to an explicitly selected
replication configuration, not as an automatic recovery policy for lost CDC identity. A deployment provides the point
at which PowerSync can validate the desired table set, capture schema, replica identity, and sync queries together.
