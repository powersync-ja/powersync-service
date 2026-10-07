---
'@powersync/service-module-mssql': patch
---

Fix the initial snapshot never completing for SQL Server tables with a composite primary key, or another key type not supported for chunked snapshots, when the table has 10,000 or more rows.
