import { Alter, AST, Create, Drop, TruncateStatement, RenameStatement, DropIndexStatement } from 'node-sql-parser';

// We ignore create table statements, since even in the worst case we will pick up the changes when row events for that
// table are received.
const DDL_KEYWORDS = ['alter table', 'drop table', 'truncate table', 'rename table'];

/**
 *  Check if a query is a DDL statement that applies to tables matching any of the provided matcher functions.
 *  @param query
 *  @param matchers
 */
export function matchedSchemaChangeQuery(query: string, matchers: ((table: string) => boolean)[]) {
  // Normalize case and remove backticks for matching
  const normalizedQuery = query.toLowerCase().replace(/`/g, '');

  const isDDLQuery = DDL_KEYWORDS.some((keyword) => normalizedQuery.includes(keyword));
  if (isDDLQuery) {
    const tokens = normalizedQuery.split(/[^a-zA-Z0-9_`]+/);
    // Check if any matched table names appear in the query
    for (const token of tokens) {
      const matchFound = matchers.some((matcher) => matcher(token));
      if (matchFound) {
        return true;
      }
    }
  }

  return false;
}

// @ts-ignore
export function isTruncate(statement: AST): statement is TruncateStatement {
  // @ts-ignore
  return statement.type === 'truncate';
}

// @ts-ignore
export function isRenameTable(statement: AST): statement is RenameStatement {
  // @ts-ignore
  return statement.type === 'rename';
}

export function isAlterTable(statement: AST): statement is Alter {
  return statement.type === 'alter';
}

export function isRenameExpression(expression: any): boolean {
  return expression.resource === 'table' && expression.action === 'rename';
}

export function isColumnExpression(expression: any): boolean {
  return expression.resource === 'column';
}

export function isConstraintExpression(expression: any): boolean {
  return (
    (expression.resource === 'key' && expression.keyword === 'primary key') ||
    expression.resource === 'constraint' ||
    (expression.resource === 'index' && expression.action === 'drop')
  );
}

export function isDropTable(statement: AST): statement is Drop {
  return statement.type === 'drop' && statement.keyword === 'table';
}

export function isDropIndex(statement: AST): statement is DropIndexStatement {
  return statement.type === 'drop' && statement.keyword === 'index';
}

export function isCreateUniqueIndex(statement: AST): statement is Create {
  return statement.type === 'create' && statement.keyword === 'index' && statement.index_type === 'unique';
}
