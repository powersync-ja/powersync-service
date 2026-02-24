import { SyntacticResultSetSource } from './table.js';
import { ParsingErrorListener } from './compiler.js';
import { PreparedSubquery } from './sqlite.js';

/**
 * Utilities for resolving references in SQL statements where multiple tables are in scope.
 *
 * Tables are added to the scope when processing `FROM` clauses.
 */
export class SqlScope {
  readonly parent?: SqlScope;
  private readonly nameToResultSet = new Map<string, SyntacticResultSetSource>();
  private readonly commonTableExpressions = new Map<string, PreparedSubquery>();

  constructor(options: { parent?: SqlScope }) {
    this.parent = options.parent;
  }

  get rootScope(): SqlScope {
    let maybeRoot: SqlScope = this;
    while (maybeRoot.parent) {
      maybeRoot = maybeRoot.parent;
    }

    return maybeRoot;
  }

  /**
   * The default result set that unqualified references resolve to.
   */
  get defaultResultSet(): SyntacticResultSetSource | null {
    if (this.nameToResultSet.size == 0) {
      return this.parent?.defaultResultSet ?? null;
    } else if (this.nameToResultSet.size == 1) {
      return this.nameToResultSet.values().next().value!;
    } else {
      return null;
    }
  }

  registerResultSet(errors: ParsingErrorListener, name: string, source: SyntacticResultSetSource) {
    const lower = name.toLowerCase();
    if (this.nameToResultSet.has(lower)) {
      errors.report(`Table with name ${name} already exists in scope`, source.origin);
      return;
    } else {
      this.nameToResultSet.set(lower, source);
    }
  }

  resolveResultSetForReference(name: string): SyntacticResultSetSource | undefined {
    return this.nameToResultSet.get(name.toLowerCase()) ?? this.parent?.resolveResultSetForReference(name);
  }

  registerCommonTableExpression(name: string, subquery: PreparedSubquery) {
    this.commonTableExpressions.set(name.toLowerCase(), subquery);
  }

  resolveCommonTableExpression(name: string): PreparedSubquery | null {
    const inThisScope = this.commonTableExpressions.get(name.toLowerCase());
    if (inThisScope) {
      return inThisScope;
    }

    return this.parent ? this.parent.resolveCommonTableExpression(name) : null;
  }
}
