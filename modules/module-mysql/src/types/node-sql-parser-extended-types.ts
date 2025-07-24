import 'node-sql-parser';

/**
 *  Missing Type definitions for the node-sql-parser
 */
declare module 'node-sql-parser' {
  interface RenameStatement {
    type: 'rename';
    table: { db: string | null; table: string }[][];
  }

  interface TruncateStatement {
    type: 'truncate';
    keyword: 'table'; // There are more keywords possible, but we only care about 'table'
    name: { db: string | null; table: string; as: string | null }[];
  }

  // This custom type more accurately describes what the structure of a Drop statement looks like for indexes.
  interface DropIndexStatement {
    type: 'drop';
    keyword: 'index';
    table: { db: string | null; table: string };
    name: any[];
  }
}
