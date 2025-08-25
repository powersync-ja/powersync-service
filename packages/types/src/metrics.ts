export enum APIMetric {
  // Uncompressed size of synced data from PowerSync to Clients
  DATA_SYNCED_BYTES = 'powersync_data_synced_bytes_total',
  // Potentially-compressed size of data sent from PowerSync to Clients
  DATA_SENT_BYTES = 'powersync_data_sent_bytes_total',
  // Number of operations synced
  OPERATIONS_SYNCED = 'powersync_operations_synced_total',
  // Number of concurrent sync connections
  CONCURRENT_CONNECTIONS = 'powersync_concurrent_connections'
}

export enum ReplicationMetric {
  // Uncompressed size of replicated data from data source to PowerSync
  DATA_REPLICATED_BYTES = 'powersync_data_replicated_bytes_total',
  // Total number of replicated rows. Not used for pricing.
  ROWS_REPLICATED = 'powersync_rows_replicated_total',
  // Total number of replicated transactions. Not used for pricing.
  TRANSACTIONS_REPLICATED = 'powersync_transactions_replicated_total',
  // Total number of replication chunks. Not used for pricing.
  CHUNKS_REPLICATED = 'powersync_chunks_replicated_total',
  // Replication lag between the source database and PowerSync instance (estimated).
  // This is estimated, and may have delays in reporting.
  REPLICATION_LAG_SECONDS = 'powersync_replication_lag_seconds'
}

export enum StorageMetric {
  // Size of current replication data stored in PowerSync
  REPLICATION_SIZE_BYTES = 'powersync_replication_storage_size_bytes',
  // Size of operations data stored in PowerSync
  OPERATION_SIZE_BYTES = 'powersync_operation_storage_size_bytes',
  // Size of parameter data stored in PowerSync
  PARAMETER_SIZE_BYTES = 'powersync_parameter_storage_size_bytes'
}
