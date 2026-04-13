import { PGNode } from 'pgsql-ast-parser';
import { ImplicitSchemaTablePattern, SourceSchemaTable } from '../index.js';
import { SqlExpression } from '../sync_plan/expression.js';
import { MapSourceVisitor, visitExpr } from '../sync_plan/expression_visitor.js';
import { equalsIgnoringResultSetList } from './compatibility.js';
import { StableHasher } from './equality.js';
import { ColumnInRow, ExpressionInput, NodeLocations, SyncExpression } from './expression.js';
import { SingleDependencyExpression } from './filter.js';
import { PreparedSubquery } from './sqlite.js';

/**
 * A result set that a query stream selects from.
 *
 * Instances of this class represent result sets that have been added as a source set to a query, e.g. through a `FROM`
 * clause.
 *
 * Different instances of this class always refer to distinct result sets, even if the source table pattern is
 * identical. For instance, a query may join the same table multiple times. This would result in two
 * {@link PhysicalSourceResultSet} sources with the same {@link PhysicalSourceResultSet.tablePattern} that are still
 * distinct.
 */
export type SourceResultSet = PhysicalSourceResultSet | TableValuedResultSet;

/**
 * The syntactic sources of a {@link SourceResultSet} being added to a table.
 */
export class SyntacticResultSetSource {
  constructor(
    readonly origin: PGNode,
    readonly explicitName: string | null
  ) {}
}

export abstract class BaseSourceResultSet {
  constructor(readonly source: SyntacticResultSetSource) {}

  abstract get description(): string;

  /**
   * The result set used as a target during evaluation.
   *
   * For all {@link PhysicalSourceResultSet}s, this is the result set itself.
   * For table-valued functions that are attached to a physical result set, this is the physical result set they're
   * attached to.
   * For table-valued functions that need to be evaluated for connections (e.g. because they expand request data), this
   * is also the result set itself.
   */
  abstract get evaluationTarget(): SourceResultSet;

  abstract clone(references: CloneTableReferences): BaseSourceResultSet;

  static areCompatible(a: SourceResultSet, b: SourceResultSet): boolean {
    if (a === b) {
      return true;
    }

    if (a instanceof TableValuedResultSet) {
      return a.canAttachTo(b);
    } else if (b instanceof TableValuedResultSet) {
      return b.canAttachTo(a);
    } else {
      return false;
    }
  }
}

/**
 * A {@link SourceResultSet} selecting rows from a table in the source database.
 *
 * The primary result set of streams must be of this type. Also, indexed lookups can only operate on this type.
 */
export class PhysicalSourceResultSet extends BaseSourceResultSet {
  constructor(
    readonly tablePattern: ImplicitSchemaTablePattern,
    source: SyntacticResultSetSource,
    /**
     * Source tables that the {@link tablePattern} resolves to in the static schema context used when compiling sync
     * streams.
     *
     * This information must only be used to generate analysis warnings, e.g. for column references that don't exist in
     * resolved tables. It must not affect how sync streams are compiled, as that is always schema-independent.
     */
    readonly schemaTablesForWarnings: SourceSchemaTable[]
  ) {
    super(source);
  }

  get evaluationTarget(): SourceResultSet {
    return this;
  }

  get description(): string {
    return this.tablePattern.name;
  }

  clone(references: CloneTableReferences): PhysicalSourceResultSet {
    return new PhysicalSourceResultSet(
      this.tablePattern,
      references.getClonedSyntacticSource(this.source),
      this.schemaTablesForWarnings
    );
  }
}

/**
 * A {@link SourceResultSet} applying a table-valued function with inputs that all depend on either a single result set
 * or request data.
 */
export class TableValuedResultSet extends BaseSourceResultSet {
  // non-null: References inputs from a physical result set.
  // null: References connection data.
  // undefined: Static inputs only, can evaluate with a physical result set.
  #attachedToPhysicalTable: PhysicalSourceResultSet | null | undefined;

  constructor(
    readonly tableValuedFunctionName: string,
    readonly parameters: SingleDependencyExpression[],
    source: SyntacticResultSetSource
  ) {
    super(source);

    // All parameters must depend on the same result set.
    if (this.parameters.length) {
      const firstParameter = this.parameters[0];
      const resultSet = firstParameter.resultSet;
      if (resultSet instanceof TableValuedResultSet) {
        throw new Error('Table-valued functions cannot use inputs from other table-valued functions');
      }

      this.#attachedToPhysicalTable = firstParameter.dependsOnConnection ? null : (resultSet ?? undefined);
      for (const parameter of parameters) {
        if (parameter.resultSet !== resultSet) {
          throw new Error(
            'Illegal table-valued result set: All inputs must depend on a single result set or request data.'
          );
        }
      }
    }
  }

  get inputResultSet(): SourceResultSet | null {
    return this.#attachedToPhysicalTable ?? null;
  }

  get evaluationTarget(): SourceResultSet {
    return this.#attachedToPhysicalTable ?? this;
  }

  get description(): string {
    return this.tableValuedFunctionName;
  }

  canAttachTo(table: SourceResultSet) {
    if (this.#attachedToPhysicalTable === table) {
      return true;
    } else if (this.#attachedToPhysicalTable === undefined && table instanceof PhysicalSourceResultSet) {
      // This table-valued function was not previously attached to a source result set, so we can attach it to any
      // physical table. This can only be done once (because it's a single logical result set, we must not evaluate
      // the function twice), but the table we attach it to is arbitrary.
      // Attaching a static table-valued function to a result set improves the efficiency of sync plans, since it allows
      // turning parameter match clauses into static row filters that reduce the amount of buckets.
      this.#attachedToPhysicalTable = table;
      return true;
    } else {
      return false;
    }
  }

  buildBehaviorHashCode(hasher: StableHasher) {
    hasher.addString(this.tableValuedFunctionName);
    equalsIgnoringResultSetList.hash(hasher, this.parameters);
  }

  behavesIdenticalTo(other: TableValuedResultSet) {
    return (
      other.tableValuedFunctionName == this.tableValuedFunctionName &&
      equalsIgnoringResultSetList.equals(other.parameters, this.parameters)
    );
  }

  clone(references: CloneTableReferences): TableValuedResultSet {
    return new TableValuedResultSet(
      this.tableValuedFunctionName,
      this.parameters.map((p) => new SingleDependencyExpression(references.cloneSyncExpression(p.expression))),
      references.getClonedSyntacticSource(this.source)
    );
  }
}

/**
 * Clones result set instances and associated expressions.
 *
 * This is necessary to safely implement common table expressions in Sync Streams: We use the identity of
 * {@link SourceResultSet} instances to track dependencies between expressions and to convert `WHERE` clauses into a
 * querier and parameter lookups. Semantically, when a CTE is joined multiple times, each CTE acts as an independently-
 * joined subquery. To implement these semantics correctly, we need to join result set instances every time we add a
 * CTE to a query.
 */
export class CloneTableReferences {
  private syntacticClone = new Map<SyntacticResultSetSource, SyntacticResultSetSource>();
  private clonedResultSets = new Map<SourceResultSet, SourceResultSet>();

  private cloneExpressions: MapSourceVisitor<ExpressionInput, ExpressionInput>;

  constructor(originalResultSets: Map<SyntacticResultSetSource, SourceResultSet>, locations: NodeLocations) {
    this.cloneExpressions = new MapSourceVisitor<ExpressionInput, ExpressionInput>((data) => {
      if (data instanceof ColumnInRow) {
        return new ColumnInRow(data.syntacticOrigin, this.getClonedSource(data.resultSet), data.column);
      }

      return data;
    }, locations);

    for (const source of originalResultSets.keys()) {
      const clone = new SyntacticResultSetSource(source.origin, source.explicitName);
      this.syntacticClone.set(source, clone);
    }

    for (const value of originalResultSets.values()) {
      this.clonedResultSets.set(value, value.clone(this));
    }
  }

  getClonedSyntacticSource(source: SyntacticResultSetSource): SyntacticResultSetSource {
    const cloned = this.syntacticClone.get(source);
    if (cloned == null) throw new Error('Syntactic source not part of original result sets');

    return cloned;
  }

  getClonedSource(source: SourceResultSet): SourceResultSet {
    const cloned = this.clonedResultSets.get(source);
    if (cloned == null) throw new Error('Source not part of original result sets');

    return cloned;
  }

  cloneExpression(source: SqlExpression<ExpressionInput>): SqlExpression<ExpressionInput> {
    return visitExpr(this.cloneExpressions, source, null);
  }

  cloneSyncExpression(expr: SyncExpression): SyncExpression {
    return new SyncExpression(this.cloneExpression(expr.node), expr.locations);
  }

  static clonePreparedSubquery(subquery: PreparedSubquery, locations: NodeLocations): PreparedSubquery {
    const cloner = new CloneTableReferences(subquery.tables, locations);

    const resultColumns: Record<string, SqlExpression<ExpressionInput>> = {};
    const tables = new Map<SyntacticResultSetSource, SourceResultSet>();
    let where: SqlExpression<ExpressionInput> | null = null;

    for (const [name, value] of Object.entries(subquery.resultColumns)) {
      resultColumns[name] = cloner.cloneExpression(value);
    }
    subquery.tables.forEach((v, k) => {
      tables.set(cloner.getClonedSyntacticSource(k), cloner.getClonedSource(v));
    });

    if (subquery.where) {
      where = cloner.cloneExpression(subquery.where);
    }

    return {
      resultColumns,
      tables,
      where
    };
  }
}
