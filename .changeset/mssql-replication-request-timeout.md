---
'@powersync/service-module-mssql': patch
---

Fix SQL Server replication getting stuck after a large transaction, such as a bulk delete, when reading its changes takes longer than 15 seconds.
