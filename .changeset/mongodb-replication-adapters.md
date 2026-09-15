---
'@powersync/service-module-mongodb': minor
---

Add a replication adapter interface for snapshot predicates and ordered change/progress streams. Persist safe resume positions after preceding writes have flushed, including when an adapter excludes all data events. Preserve exact-token resumes within transactions and export a pluggable, disposable ChangeStreamTestContext for module integration tests.
