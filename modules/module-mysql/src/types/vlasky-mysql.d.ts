// Minimal type declarations for @vlasky/mysql, which ships without any.
// Only the surface used by this module is declared.
declare module '@vlasky/mysql' {
  import { MySQLConnection } from '@powersync/mysql-zongji';

  export interface VlaskyConnection extends MySQLConnection {
    destroy(): void;
    state: string;
  }

  export function createConnection(options: Record<string, unknown>): VlaskyConnection;
}
