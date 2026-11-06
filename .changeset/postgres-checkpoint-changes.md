---
'@powersync/service-module-postgres-storage': minor
---

Track changes between checkpoints in Postgres bucket storage instead of invalidating everything. Connections now only re-run parameter queries when one of their parameter lookups changed, found with a range query on `bucket_parameters` (its ids share `op_id_sequence` with `bucket_data`) and shared between connections through an LRU cache, like MongoDB storage. With the optional index `CREATE INDEX CONCURRENTLY bucket_data_op_id_index ON bucket_data (group_id, op_id)`, only the data buckets written between two checkpoints are re-checked as well. Large ranges (over 1,000 lookups or 10,000 operations) still invalidate everything.
