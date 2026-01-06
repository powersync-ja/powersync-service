import { astVisitor, ExprRef, PGNode } from 'pgsql-ast-parser';
import { SyntacticResultSetSource } from '../ir/table.js';
import { ParsingErrorListener } from './compiler.js';

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
