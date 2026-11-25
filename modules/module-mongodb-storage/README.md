# PowerSync Service Module MongoDB Storage

MongoDB bucket storage module for PowerSync

## Azure Blob object storage

Azure Blob Storage can hold offloaded bucket-data chunks in place of S3. MongoDB
still stores replication state, indexes, and object references; this is not a
replacement for MongoDB storage. It uses the existing chunk storage implementation
for storage versions 3 and 4.

Create a private Blob container before starting the service. For Azure deployments,
use an account URL and grant the service's identity **Storage Blob Data Contributor**
on that container:

```yaml
storage:
  type: mongodb
  uri: !env PS_MONGO_URI
  object_storage:
    type: azure
    account_url: https://myaccount.blob.core.windows.net
    container: powersync
    prefix: my-instance
```

`account_url` uses `DefaultAzureCredential` from `@azure/identity`, supporting
workload identity, managed identity, environment credentials, and local Azure CLI
credentials. Set `AZURE_CLIENT_ID` to select a user-assigned managed identity.
Alternatively, replace `account_url` with
`connection_string: !env PS_AZURE_STORAGE_CONNECTION_STRING` for shared-key or SAS
authentication. Exactly one of these two settings is required. The service does
not create containers or manage role assignments.

Use a separate container or prefix for each service instance. Object references
contain relative paths: switching an existing instance from S3 to Azure requires
copying all referenced objects with their content headers and paths preserved,
or rebuilding the service's stored data. Changing the provider alone does not
migrate objects.

Optional settings:

| Setting                  | Default | Meaning                                                                                                                                                             |
| ------------------------ | ------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `concurrency_limit`      | `64`    | Shared limit across uploads, downloads, list pages, and delete batches.                                                                                             |
| `operation_timeout_ms`   | `60000` | Deadline per operation, including authentication, SDK retries, and response streaming. Waiting for a concurrency slot has a separate deadline of the same duration. |
| `inline_threshold_bytes` | `1024`  | Chunks smaller than this BSON size remain inline in MongoDB.                                                                                                        |

The Azure SDK uses exponential retries with up to three attempts and a per-attempt
timeout of 20 seconds (or `operation_timeout_ms`, if smaller). The overall deadline
also bounds retries and stalled response bodies. Transient network failures,
throttling, and service failures are classified for compaction retries; missing
objects and authorization failures are not. Delete requests use batches of up to
256 blobs and check every subresponse. Missing blobs can safely be deleted again.

## Local testing with Azurite

[Azurite](https://github.com/Azure/Azurite) provides local Azure Blob Storage without
an Azure subscription. Start its Blob service:

```sh
docker run --rm --name powersync-azurite -p 127.0.0.1:10000:10000 \
  mcr.microsoft.com/azure-storage/azurite:3.38.0 \
  azurite-blob --blobHost 0.0.0.0 --disableTelemetry
```

Run the adapter tests and shared storage integration tests from this repository:

```sh
AZURITE_CONNECTION_STRING='UseDevelopmentStorage=true' \
  pnpm --filter './modules/module-mongodb-storage' test \
  test/src/storage_azure.test.ts test/src/storage_s3_reading.test.ts \
  test/src/storage_s3_writing.test.ts test/src/storage_s3_checksums.test.ts \
  test/src/storage_s3_compaction_lifecycle.test.ts --run
```

The shared integration tests also require the MongoDB replica set described in
[DEVELOP.md](../../DEVELOP.md). The test setup creates a dedicated container and
isolates tests with unique prefixes. Unset `RUSTFS_ENDPOINT` when using Azurite.
Without either environment variable, shared tests use in-memory object storage.
CI runs these tests against both RustFS/S3 and Azurite.

For a local service, create a container and use `type: azure`, its `container` name,
and `connection_string: UseDevelopmentStorage=true`. This shorthand connects to
loopback; services running inside Docker need an explicit connection string whose
Blob endpoint points to the Azurite container. Azurite emulates storage APIs, so
production identity/RBAC, network policies, and performance still need validation
against an Azure account.
