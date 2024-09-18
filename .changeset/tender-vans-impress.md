---
'@powersync/service-core': minor
'@powersync/service-sync-rules': minor
'@powersync/lib-services-framework': minor
'@powersync/service-jpgwire': minor
'@powersync/service-types': minor
'@powersync/service-image': minor
'@powersync/service-module-postgres': patch
---

- Introduced modules to the powersync service architecture
  - Core functionality has been moved to "engine" classes. Modules can register additional functionality with these engines.
  - The sync API functionality used by the routes has been abstracted to an interface. API routes are now managed by the RouterEngine.
  - Replication is managed by the ReplicationEngine and new replication data sources can be registered to the engine by modules.
- Refactored existing Postgres replication as a module.
- Removed Postgres specific code from the core service packages.
