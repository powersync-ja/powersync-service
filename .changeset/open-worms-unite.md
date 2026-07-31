---
'@powersync/service-module-mssql': minor
---

MSSQL CDCPoller improvements and fixes:
- Ensure correct ordering of CDC results which previously could cause inconsistencies when handling deferred updates
- Correctly count processed transactions in each polling cycle
