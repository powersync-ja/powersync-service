---
'@powersync/lib-service-postgres': patch
'@powersync/service-jpgwire': patch
'@powersync/lib-service-mongodb': patch
'@powersync/service-module-mongodb': patch
'@powersync/service-module-mysql': patch
---

Support connection parameters via database URL query string. PostgreSQL supports `connect_timeout`. MongoDB supports `connectTimeoutMS`, `socketTimeoutMS`, `serverSelectionTimeoutMS`, `maxPoolSize`, `maxIdleTimeMS`. MySQL supports `connectTimeout`, `connectionLimit`, `queueLimit`.
