import { TablePattern } from '@powersync/service-sync-rules';
import * as types from '@powersync/service-types';

/**
 *  Describes all the methods currently required to service the sync API endpoints.
 *  TODO: This interface needs to be cleaned up and made more generic. It describes the current functionality required by our API routes
 */
export interface SyncAPI {
  /**
   * Performs diagnostics on the "connection"
   * This is usually some test query to verify the source can be reached.
   */
  getDiagnostics(): Promise<{
    connected: boolean;
    errors?: Array<{ level: string; message: string }>;
  }>;

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
  getReplicationLag(): Promise<number>;

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
   * Executes a SQL statement and returns the result. This is currently used in the
   * admin API which is exposed in Collide.
   */
  executeSQL(sql: string, params: any[]): Promise<types.internal_routes.ExecuteSqlResponse>;

  //CRUD API : I don't think this is used besides maybe niche dev use cases
}

// TODO: Export this when the existing definition in WALConnection is removed
interface PatternResult {
  schema: string;
  pattern: string;
  wildcard: boolean;
  tables?: types.TableInfo[];
  table?: types.TableInfo;
}
