import { astVisitor, ExprRef, PGNode } from 'pgsql-ast-parser';
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

  /**
   * Returns a visitor binding references in AST nodes it runs on to this scope.
   *
   * This is used to preserve scopes when smuggling references out of subqueries. The parser transforms subqueries from
   * `WHERE outer = (SELECT inner FROM a ...)` to `JOIN a ON ... AND outer = inner`. In the transformed form, we need
   * to preserve original scopes for `outer` and `inner`.
   */
  bindingVisitor(root: PGNode) {
    return astVisitor((v) => ({
      ref: (expr) => {
        (expr as any)[boundScope] = this;
      },
      select(stmt) {
        if (stmt !== root) {
          // Return and don't visit children (since those have their own scope).
          return;
        } else {
          v.super().select(stmt);
        }
      },
      statement(stmt) {
        if (stmt !== root) {
          // Return and don't visit children (since those have their own scope).
          return;
        } else {
          v.super().statement(stmt);
        }
      }
    }));
  }

  /**
   * If the given node was part of an AST previously passed to {@link bindTo}, returns the
   * scope attached to it.
   */
  static readBoundScope(node: ExprRef): SqlScope | undefined {
    return (node as any)[boundScope];
  }
}

const boundScope = Symbol('SqlScope.boundScope');
