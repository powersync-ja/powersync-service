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

  get description(): string {
    return this.tablePattern.name;
  }
}

/**
 * A {@link SourceResultSet} applying a table-valued function with inputs that all depend on either a single result set
 * or request data.
 */
export class TableValuedResultSet extends BaseSourceResultSet {
  constructor(
    readonly tableValuedFunctionName: string,
    readonly parameters: SingleDependencyExpression[],
    source: SyntacticResultSetSource
  ) {
    super(source);

    // All parameters must depend on the same result set.
    const resultSet = this.inputResultSet;
    for (const parameter of parameters) {
      if (parameter.resultSet !== resultSet) {
        throw new Error(
          'Illegal table-valued result set: All inputs must depend on a single result set or request data.'
        );
      }
    }
  }

  get inputResultSet(): SourceResultSet | null {
    // It's the same for all inputs, validated in the constructor
    if (this.parameters.length) {
      return this.parameters[0].resultSet;
    }

    return null;
  }

  get description(): string {
    return this.tableValuedFunctionName;
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
