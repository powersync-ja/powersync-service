---
'@powersync/service-module-mysql': patch
---

Fix checkpoints stalling on idle MySQL servers. Heartbeat keepalives now report the LSN of the last committed transaction instead of the transaction start position, which previously blocked checkpoint creation ("Waiting before creating checkpoint" logged every ~30s) until the next transaction arrived.
