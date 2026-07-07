import { SyncConfig, TablePattern } from '@powersync/service-sync-rules';
import * as types from '@powersync/service-types';
import { ParseSyncConfigOptions, SyncRulesBucketStorage } from '../storage/storage-index.js';

export interface PatternResult {
  schema: string;
  pattern: string;
  wildcard: boolean;
  tables?: types.TableInfo[];
  table?: types.TableInfo;
}

export interface ReplicationLagOptions {
  bucketStorage: SyncRulesBucketStorage;
}

export interface SlotWalBudgetInfo {
  wal_status: string;
  safe_wal_size?: number;
  max_slot_wal_keep_size?: number;
}

export interface SlotWalBudgetOptions {
  slotName: string;
}

/**
 * Source-specific capabilities used by admin routes, sync config validation,
 * checkpointing, diagnostics, and the sync API.
 */
export interface RouteAPI {
  /**
   * @returns basic identification of the connection
   */
  getSourceConfig(): Promise<types.configFile.ResolvedDataSourceConfig>;

  /**
   * Checks the current connection status of the data source.
   * This is usually some test query to verify the source can be reached.
   */
  getConnectionStatus(): Promise<types.ConnectionStatusV2>;

  /**
   * Expand sync config table patterns into table metadata from the source connection.
   *
   * This is used by validation and diagnostics paths to explain what will be
   * replicated for the current sync config.
   *
   * @param tablePatterns A set of table patterns which typically come from
   *          the tables listed in sync config definitions.
   *
   * @param sqlSyncRules
   * @returns Matching tables or collections, their columns or fields where available,
   *          and per-table warnings or errors for the requested table patterns.
   */
  getDebugTablesInfo(tablePatterns: TablePattern[], sqlSyncRules: SyncConfig): Promise<PatternResult[]>;

  /**
   * @returns The replication lag: that is the amount of data which has not been
   *          replicated yet, in bytes.
   */
  getReplicationLagBytes(options: ReplicationLagOptions): Promise<number | undefined>;

  /**
   * @returns WAL budget information for the replication slot, or undefined
   *          if the slot doesn't exist or the source doesn't support this.
   *          Only implemented by the Postgres adapter.
   */
  getSlotWalBudget?(options: SlotWalBudgetOptions): Promise<SlotWalBudgetInfo | undefined>;

  /**
   * Get the current LSN or equivalent replication HEAD position identifier.
   *
   * The position is provided to the callback so the caller can persist its
   * write-checkpoint mapping before the source adapter forces any required
   * source-side marker or keepalive. The callback returns whether storage
   * actually needs a source marker: when `shouldAdvance` is true the adapter
   * must ensure that the replication stream will observe this position or a
   * greater one, even when the source is otherwise idle. When it is false, the
   * adapter may skip the source marker.
   *
   * Reading the head and forcing the marker happen within a single source
   * session/connection where applicable, so the marker is causally ordered
   * after the head that was handed to the callback.
   */
  createReplicationHead<T>(callback: ReplicationHeadCallback<T>): Promise<T>;

  /**
   * @returns The connected source schema in service-friendly table and column
   *          formats. Strict-schema sources should include actual tables,
   *          collections, columns, and types. Schemaless sources should return
   *          the best metadata available and document validation limitations in
   *          their adapter behavior. This is typically used to validate sync
   *          config and generate client schemas.
   */
  getConnectionSchema(): Promise<types.DatabaseSchema[]>;

  /**
   * Executes a query and return the result from the data source. This is currently used in the
   * admin API which is exposed in Collide.
   */
  executeQuery(query: string, params: any[]): Promise<types.internal_routes.ExecuteSqlResponse>;

  /**
   * Close any resources that need graceful termination.
   */
  shutdown(): Promise<void>;

  /**
   * Get source-specific sync config parsing defaults, such as the default
   * schema or database to use when sync config references an unqualified table
   * name.
   *
   * This is used wherever sync config is parsed for this source, including
   * validation, diagnostics, and sync routing.
   */
  getParseSyncRulesOptions(): ParseSyncConfigOptions;
}

export interface ReplicationHeadResult<T> {
  response: T;
  /**
   * True when storage needs the source to force a later observable position.
   * False when storage can prove no marker is needed.
   */
  shouldAdvance: boolean;
}

export type ReplicationHeadCallback<T> = (head: string) => Promise<ReplicationHeadResult<T>>;
