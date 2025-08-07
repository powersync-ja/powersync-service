---
'@powersync/service-module-mysql': minor
'@powersync/service-module-postgres-storage': patch
'@powersync/service-module-mongodb-storage': patch
'@powersync/service-core': patch
---

- Hooked up the MySQL binlog heartbeat events with the bucket batch keepalive mechanism. 
  Heartbeat events will now update the latest keepalive timestamp in the sync rules.
