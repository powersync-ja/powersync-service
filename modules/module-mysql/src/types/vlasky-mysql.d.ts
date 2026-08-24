// Minimal type declarations for @vlasky/mysql, which ships without any.
// Only the surface used by this module is declared.
declare module '@vlasky/mysql' {
  import { MySQLConnection } from '@powersync/mysql-zongji';

  export interface VlaskyConnection extends MySQLConnection {
    destroy(): void;
    state: string;
    /**
     * Options form of query. The driver starts `timeout` when the query begins executing, not when
     * it is queued behind other queries on the connection.
     */
    query(options: { sql: string; timeout?: number }, callback: (error: any, results: any, fields: any) => void): void;
  }

  export function createConnection(options: Record<string, unknown>): VlaskyConnection;
}
