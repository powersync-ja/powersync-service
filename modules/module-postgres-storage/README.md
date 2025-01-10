# Postgres Sync Bucket Storage

This module provides a `BucketStorageProvider` which uses a Postgres database for persistence.

## Configuration

Postgres storage can be enabled by selecting the appropriate storage `type` and providing connection details for a Postgres server.

The storage connection configuration extends the configuration for a Postgres replication source, thus it accepts and supports the same configurations fields.

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
A separate Postgres server is currently required for replication connections (if using Postgres for replication) and storage. Using the same server might cause unexpected results.

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

### Batching

Replication data is persisted via batch operations. Batching ensures performant, memory optimized writes. Batches are limited in size. Increasing batch size limits could reduce the amount of server round-trips which could increase performance (in some circumstances), but will result in higher memory usage and potential server issues.

Batch size limits are defaulted and can optionally be configured in the configuration.

```yaml
# Connection settings for sync bucket storage
storage:
  type: postgresql
  # This accepts the same parameters as a Postgres replication source connection
  uri: !env PS_STORAGE_SOURCE_URI
  batch_limits:
    # Maximum estimated byte size of operations in a single batch.
    # Defaults to 5 megabytes.
    max_estimated_size: 5000000
    # Maximum number of records present in a single batch.
    # Defaults to 2000 records.
    # Increasing this limit can improve replication times for large volumes of data.
    max_record_count: 2000
    # Maximum byte size of size of current_data documents we lookup at a time.
    # Defaults to 50 megabytes.
    max_current_data_batch_size: 50000000
```
