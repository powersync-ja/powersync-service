import { PGNode } from 'pgsql-ast-parser';
import { RequestExpression, SingleDependencyExpression } from './filter.js';
import { StableHasher } from './equality.js';
import { equalsIgnoringResultSetList } from './compatibility.js';
import { ImplicitSchemaTablePattern, SourceSchemaTable } from '../index.js';

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

  static areCompatible(a: SourceResultSet, b: SourceResultSet): boolean {
    if (a instanceof TableValuedResultSet) {
      return a.canAttachTo(b);
    } else if (b instanceof TableValuedResultSet) {
      return b.canAttachTo(a);
    } else {
      return a === b;
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
}
