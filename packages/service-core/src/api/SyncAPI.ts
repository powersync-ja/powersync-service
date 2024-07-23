import { TablePattern } from '@powersync/service-sync-rules';
import * as types from '@powersync/service-types';

/**
 *  Describes all the methods currently required to service the sync API endpoints.
 */
export interface SyncAPI {
  /**
   * @returns basic identification of the connection
   */
  getSourceConfig(): Promise<types.configFile.DataSourceConfig>;

  /**
   * Checks the current connection status of the datasource.
   * This is usually some test query to verify the source can be reached.
   */
  getConnectionStatus(): Promise<ConnectionStatusResponse>;

  /**
   * Generates replication table information from a given pattern of tables.
   *
   * @param tablePatterns A set of table patterns which typically come from
   *          the tables listed in sync rules definitions.
   *
   * @returns A result of all the tables and columns which should be replicated
   *           based off the input patterns. Certain tests are executed on the
   *           tables to ensure syncing should function according to the input
   *           pattern. Debug errors and warnings are reported per table.
   */
  getDebugTablesInfo(tablePatterns: TablePattern[]): Promise<PatternResult[]>;

  /**
   * @returns The replication lag: that is the amount of data which has not been
   *          replicated yet, in bytes.
   */
  getReplicationLag(slotName: string): Promise<number>;

  /**
   * Get the current LSN or equivalent replication position identifier
   */
  getCheckpoint(): Promise<bigint>;

  /**
   * @returns The schema for tables inside the connected database. This is typically
   *          used to validate sync rules.
   * Side Note: https://github.com/powersync-ja/powersync-service/blob/33bbb8c0ab1c48555956593f427fc674a8f15768/packages/types/src/definitions.ts#L100
   * contains `pg_type` which we might need to deprecate and add another generic
   * type field - or just use this field as the connection specific type.
   */
  getConnectionSchema(): Promise<types.DatabaseSchema[]>;

  /**
   * Executes a query and return the result from the data source. This is currently used in the
   * admin API which is exposed in Collide.
   */
  executeQuery(query: string, params: any[]): Promise<types.internal_routes.ExecuteSqlResponse>;

  /**
   * The management service and SDK expose a demo credentials endpoint.
   * Not sure if this is actually used.
   */
  getDemoCredentials(): Promise<DemoCredentials>;

  //CRUD API : I don't think this is used besides maybe niche dev use cases

  /**
   * Close any resources that need graceful termination.
   */
  shutdown(): Promise<void>;
}

export interface DemoCredentials {
  url: string;
}

// TODO: Export this when the existing definition in WALConnection is removed
interface PatternResult {
  schema: string;
  pattern: string;
  wildcard: boolean;
  tables?: types.TableInfo[];
  table?: types.TableInfo;
}

interface ErrorDescription {
  level: string;
  message: string;
}
export interface ConnectionStatusResponse {
  connected: boolean;
  errors?: ErrorDescription[];
}

export interface QueryResults {
  columns: string[];
  rows: (string | number | boolean | null)[][];
}
