---
'@powersync/service-module-postgres-storage': patch
'@powersync/service-core': patch
---

Improve Postgres compaction performance by replacing bucket range filtering with incremental discovery and per-bucket exact-match queries, eliminating `COLLATE "C"` and `U+FFFF` sentinel usage.

- The compactor logic is an internal refactor that improves query performance (no API change)
- The CLI change adds validation but is a correction (prefix matching was broken/unused anyway)
- The `CompactOptions.compactBuckets` JSDoc is a documentation-only update

Resolves #400.
