---
'@powersync/service-types': patch
'@powersync/service-core': patch
'@powersync/service-module-postgres': patch
'@powersync/service-errors': patch
---

Detect WAL slot invalidation mid-snapshot, warn on WAL budget depletion, block futile retries, and surface WAL budget in the diagnostics API.
