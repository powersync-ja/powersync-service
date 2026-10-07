---
'@powersync/service-module-mongodb': minor
'@powersync/service-errors': minor
---

Add a replication adapter interface for snapshot predicates and ordered change/progress streams. Persist safe resume positions after preceding writes have flushed, including when an adapter excludes all data events. Preserve exact-token resumes within transactions and export a pluggable, disposable ChangeStreamTestContext for module integration tests.

Report a replication error if a saved sync config requires MongoDB pre-filtering but no query provider factory is registered. Restore the required module, or remove the filters and deploy a new sync config to restart replication.
