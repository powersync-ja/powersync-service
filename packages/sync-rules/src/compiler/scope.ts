import { SyntacticResultSetSource } from './table.js';
import { ParsingErrorListener } from './compiler.js';

/**
 * Utilities for resolving references in SQL statements where multiple tables are in scope.
 *
 * Tables are added to the scope when processing `FROM` clauses.
 */
export class SqlScope {
  readonly parent?: SqlScope;
  private readonly nameToResultSet = new Map<string, SyntacticResultSetSource>();

  constructor(options: { parent?: SqlScope }) {
    this.parent = options.parent;
  }

  get resultSets(): SyntacticResultSetSource[] {
    return [...this.nameToResultSet.values()];
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
}
