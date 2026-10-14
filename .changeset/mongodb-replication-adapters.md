---
'@powersync/service-module-mongodb': minor
'@powersync/service-errors': minor
---

Add a replication adapter interface for snapshot predicates and ChangeStream event batches. Both default and filtering providers parse events incrementally with shared split-event assembly. Persist safe resume positions after preceding writes have flushed, including when an adapter excludes all data events. Preserve exact-token resumes within transactions and export a pluggable, disposable ChangeStreamTestContext for module integration tests.

Expose a shared filtering helper with typed options and keep/exclude/delete decisions. The helper preserves checkpoint and DDL events, source batch boundaries and safe resume progress while counting exclusions. Export the shared stream and filter types from the replication module.

Report a replication error if a saved sync config requires MongoDB pre-filtering but no query provider factory is registered. Restore the required module, or remove the filters and deploy a new sync config to restart replication.
