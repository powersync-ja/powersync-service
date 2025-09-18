# Postgres Sync Bucket Storage

This module provides a `BucketStorageProvider` which uses a Postgres database for persistence.

## Configuration

Postgres storage can be enabled by selecting the appropriate storage `type` and providing connection details for a Postgres server.

The storage connection configuration supports the same fields as the Postgres replication connection configuration.

A sample YAML configuration could look like

```yaml
replication:
  # Specify database connection details
  # Note only 1 connection is currently supported
  # Multiple connection support is on the roadmap
  connections:
    - type: postgresql
      uri: !env PS_DATA_SOURCE_URI

# Connection settings for sync bucket storage
storage:
  type: postgresql
  # This accepts the same parameters as a Postgres replication source connection
  uri: !env PS_STORAGE_SOURCE_URI
```

**IMPORTANT**:

Separate Postgres servers are required for replication connections **if using PostgreSQL versions below 14**.

| PostgreSQL Version | Server configuration                                                                                                                                                                                                     |
| ------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| Below 14           | Separate servers are required for the source and sync bucket storage. Replication will be blocked if the same server is detected.                                                                                        |
| 14 and above       | The source database and sync bucket storage database can be on the same server. Using the same database (with separate schemas) is supported but may lead to higher CPU usage. Using separate servers remains an option. |

### Connection credentials

The Postgres bucket storage implementation requires write access to the provided Postgres database. The module will create a `powersync` schema in the provided database which will contain all the tables and data used for bucket storage. Ensure that the provided credentials specified in the `uri` or `username`, `password` configuration fields have the appropriate write access.

A sample user could be created with the following

If a `powersync` schema should be created manually

```sql
CREATE USER powersync_storage_user
WITH
  PASSWORD 'secure_password';

CREATE SCHEMA IF NOT EXISTS powersync AUTHORIZATION powersync_storage_user;

GRANT CONNECT ON DATABASE postgres TO powersync_storage_user;

GRANT USAGE ON SCHEMA powersync TO powersync_storage_user;

GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA powersync TO powersync_storage_user;
```

If the PowerSync service should create a `powersync` schema

```sql
CREATE USER powersync_storage_user
WITH
  PASSWORD 'secure_password';

-- The user should only have access to the schema it created
GRANT CREATE ON DATABASE postgres TO powersync_storage_user;
```
