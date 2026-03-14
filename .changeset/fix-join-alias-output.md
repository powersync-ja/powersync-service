---
'@powersync/service-sync-rules': patch
---

Fixed JOIN queries with aliased source tables producing incorrect output table names in edition 3 compiled streams. Table aliases in JOINs are now correctly treated as SQL disambiguation rather than output table renames.
