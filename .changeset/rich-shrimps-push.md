---
'@powersync/service-sync-rules': minor
'@powersync/service-image': minor
---

Add the `versioned_bucket_ids` option in the `config:` block for sync rules. When enabled, generated bucket ids include the version of sync rules. This allows clients to sync more efficiently after updating sync rules.
