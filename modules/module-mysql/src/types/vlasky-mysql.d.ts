// Minimal type declarations for @vlasky/mysql, which ships without any.
// Only the surface used by this module is declared; the connection type
// comes from @powersync/mysql-zongji.
declare module '@vlasky/mysql' {
  import { MySQLConnection } from '@powersync/mysql-zongji';

  export function createConnection(options: Record<string, unknown>): MySQLConnection;
}
