---
'@powersync/service-sync-rules': patch
---

Surface joining onto an aliased primary table as a non-fatal warning in the Sync Streams compiler. The stream
compiles successfully, but filter expressions that reference the joined table cannot be resolved through the
alias and the stream may silently sync zero rows. Drop the alias on the primary table, or quote it
(`FROM user_data AS "users", ...`) to acknowledge the constraint and suppress the warning. Addresses #565.
