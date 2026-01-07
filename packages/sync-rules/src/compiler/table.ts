import { PGNode } from 'pgsql-ast-parser';
import { RequestExpression } from './filter.js';
import { StableHasher } from './equality.js';
import { equalsIgnoringResultSetList } from './compatibility.js';

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
export type SourceResultSet = PhysicalSourceResultSet | RequestTableValuedResultSet;

/**
 * The syntactic sources of a {@link SourceResultSet} being added to a table.
 */
export class SyntacticResultSetSource {
  constructor(readonly origin: PGNode) {}
}

abstract class BaseSourceResultSet {
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
    readonly tablePattern: string,
    source: SyntacticResultSetSource
  ) {
    super(source);
  }

  get description(): string {
    return this.tablePattern;
  }
}

/**
 * A {@link SourceResultSet} applying a table-valued function with inputs that exclusively depend on request data.
 */
export class RequestTableValuedResultSet extends BaseSourceResultSet {
  constructor(
    readonly tableValuedFunctionName: string,
    readonly parameters: RequestExpression[],
    source: SyntacticResultSetSource
  ) {
    super(source);
  }

  get description(): string {
    return this.tableValuedFunctionName;
  }

  buildBehaviorHashCode(hasher: StableHasher) {
    hasher.addString(this.tableValuedFunctionName);
    equalsIgnoringResultSetList.hash(hasher, this.parameters);
  }

  behavesIdenticalTo(other: RequestTableValuedResultSet) {
    return (
      other.tableValuedFunctionName == this.tableValuedFunctionName &&
      equalsIgnoringResultSetList.equals(other.parameters, this.parameters)
    );
  }
}
