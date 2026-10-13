---
'@powersync/service-module-mongodb': minor
'@powersync/service-errors': minor
---

Add a replication adapter interface for snapshot predicates and ChangeStream event batches. Both default and filtering providers parse events incrementally with shared split-event assembly.

Report a replication error if a saved sync config requires MongoDB pre-filtering but no query provider factory is registered. Restore the required module, or remove the filters and deploy a new sync config to restart replication.
