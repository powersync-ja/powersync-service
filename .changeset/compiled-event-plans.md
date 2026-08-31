---
'@powersync/service-sync-rules': minor
'@powersync/service-core': minor
---

Compile event payload queries into serialized sync plans and evaluate them through the shared row-projection implementation. Existing event deployments use one basic, direct-projection query per physical table; filters and other advanced SQL forms are not currently documented or recommended.

Edition 3 now validates and applies event `WHERE` clauses. Existing persisted plans and editions 1 and 2 retain the legacy behavior of ignoring filters and emit a warning; deploy a new sync config to enable compiled filtering. Remove event `WHERE` clauses before rolling the service back to an older version.

Event evaluation now follows sync stream semantics by returning all payloads from matching queries, including overlapping exact and wildcard sources, or no payloads when filters do not match. Payload query order does not affect persisted equality. Compiled `SELECT *` payloads also include underscore-prefixed columns, matching sync streams.
