import { And, BaseTerm, EqualsClause, isBaseTerm, Or, SingleDependencyExpression } from './filter.js';
import { NodeLocations, SyncExpression } from './expression.js';
import { SourceResultSet } from './table.js';
import { BinaryOperator, SqlExpression } from '../sync_plan/expression.js';
import { expandNodeLocations } from '../errors.js';

export class FilterConditionSimplifier {
  simplifyOr(or: Or): Or {
    const andTerms: And[] = [];
    let baseTerms: BaseTerm[] = [];

    for (const term of or.terms) {
      const simplified = this.simplifyAnd(term);
      if (isBaseTerm(simplified)) {
        baseTerms.push(simplified);
      } else {
        andTerms.push(simplified);
      }
    }

    baseTerms = this.mergeByCommonDependencies('or', baseTerms);
    for (const term of baseTerms) {
      andTerms.push({ terms: [term] });
    }

    return { terms: andTerms };
  }

  private simplifyAnd(and: And): And | BaseTerm {
    const merged = this.mergeByCommonDependencies('and', and.terms);

    if (merged.length == 1) {
      return merged[0];
    }
    return { terms: merged };
  }

  /**
   * Re-orders terms based on their dependencies, and then uses the given operator to merge expressions with the same
   * dependency set.
   *
   * This is used to push boolean expressions down if they can be evaluated on the same table. For instance, the and
   * terms `[row.foo, row.bar]` can be represented as a single base term `row.foo AND row.bar`. Pushing operators down
   * into the expression generally reduces the complexity of the sync plan. In particular for `OR` terms, it can also
   * reduce the amount of buckets since each term in an `OR` is implemented as another bucket.
   */
  private mergeByCommonDependencies(operator: BinaryOperator, baseTerms: BaseTerm[]): BaseTerm[] {
    const byResultSet = new Map<SourceResultSet, SingleDependencyExpression[]>();
    const noResultSet: SingleDependencyExpression[] = [];
    const rest: BaseTerm[] = [];

    for (const term of baseTerms) {
      const simplified = this.simplifyBase(term);
      if (simplified instanceof SingleDependencyExpression) {
        if (simplified.resultSet != null) {
          if (byResultSet.has(simplified.resultSet)) {
            byResultSet.get(simplified.resultSet)!.push(simplified);
          } else {
            byResultSet.set(simplified.resultSet, [simplified]);
          }
        } else {
          noResultSet.push(simplified);
        }
      } else {
        rest.push(term);
      }
    }

    const addMerged = (elements: SingleDependencyExpression[]) => {
      if (elements.length == 0) {
        return;
      }

      rest.push(this.composeExpressions(operator, ...elements));
    };
    addMerged(noResultSet);
    for (const terms of byResultSet.values()) {
      addMerged(terms);
    }

    return rest;
  }

  private simplifyBase(base: BaseTerm): BaseTerm {
    if (base instanceof EqualsClause) {
      // If the left and right terms have shared dependencies, we shouldn't represent this as an equals clause. For
      // instance, terms like `notes.state = 'public'` are generated as an equals clause initially but they can just be
      // a row condition that is much cheaper to compute than a static bucket parameter. Similarly, `row.foo = row.bar`
      // must be a row condition since it can't be represented as parameters that could be instantiated.
      if (
        SingleDependencyExpression.extractSingleDependency([
          ...base.left.expression.instantiation,
          ...base.right.expression.instantiation
        ])
      ) {
        return this.composeExpressions('=', base.left, base.right);
      }
    }

    return base;
  }

  /**
   * Reduces expressions through a chain of binary operators.
   *
   * For instance, `composeExpressions('AND', a, b, c)` returns `a AND b AND c` as a single expression. All expressions
   * must have compatible dependencies.
   */
  private composeExpressions(
    operator: BinaryOperator,
    ...terms: SingleDependencyExpression[]
  ): SingleDependencyExpression {
    if (terms.length == 0) {
      throw new Error("Can't compose zero expressions");
    }

    const locations = terms[0].expression.locations;
    const inner = composeExpressionNodes(
      locations,
      operator,
      terms.map((t) => t.expression.node)
    );

    return new SingleDependencyExpression(new SyncExpression(inner, locations));
  }
}

export function composeExpressionNodes<T>(
  locations: NodeLocations,
  operator: BinaryOperator,
  terms: SqlExpression<T>[]
) {
  if (terms.length == 0) {
    throw new Error("Can't compose zero expressions");
  }

  const [first, ...rest] = terms;
  let inner = first;
  for (const additional of rest) {
    inner = { type: 'binary', operator, left: inner, right: additional };
  }

  const location = expandNodeLocations(terms.map((e) => locations.locationFor(e).location));
  if (location) {
    locations.sourceForNode.set(inner, {
      location,
      errors: locations.locationFor(terms[0]).errors
    });
  }

  return inner;
}
