---
'@powersync/service-sync-rules': minor
'@powersync/service-core': minor
---

Compile replication events into serialized sync-plan data, evaluate them through the shared row-projection implementation, and expose behavioral equality for persisted event plans. Raw event SQL remains available as a compatibility mirror for older services.

The legacy event evaluator silently ignored `WHERE` clauses and emitted an event payload for every row. Edition 3 deployments now validate and apply those filters (if present). Existing persisted plans without compiled events continue to ignore them because applying the new validation during a service upgrade could reject previously accepted SQL and interrupt an existing deployment. Redeploying the sync config validates and enables its filters. Editions 1 and 2 also retain the legacy behavior and emit a warning because they do not persist compiled plans.

Compiled event plans remain additive to sync-plan versions 1 and 2 for rolling-upgrade compatibility. Older service instances use the raw SQL mirror while reading a newly compiled plan, retaining the legacy behavior of ignoring event payload filters and selecting only the first payload query for a source table. Upgrade all service instances before deploying a sync config that relies on event filters or multiple payload queries for the same table.

When an edition 3 event payload query has a `WHERE` clause, rows that do not match it no longer produce an event payload. This means an evaluated event may return no result without reporting an error.

The legacy evaluator excluded source columns prefixed with `_` from `SELECT *` event payloads. Compiled event payloads now include those columns, matching the existing behavior of sync stream projections.
