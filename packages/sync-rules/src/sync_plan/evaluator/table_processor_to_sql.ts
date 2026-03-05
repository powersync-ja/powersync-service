import {
  mapExternalDataToInstantiation,
  TableValuedFunction,
  TableValuedFunctionOutput
} from '../engine/scalar_expression_engine.js';
import { SqlExpression } from '../expression.js';
import { MapSourceVisitor, visitExpr } from '../expression_visitor.js';
import * as plan from '../plan.js';

/**
 * Utility to translate a {@link plan.TableProcessor} to a scalar SQL statement.
 *
 * This translates table-valued functions and filters. Output columns and partition keys have to be translated
 * separately, because their order depends on the type of table processor (data source vs. parameter lookup creator).
 */
export class TableProcessorToSqlHelper {
  mapper = mapExternalDataToInstantiation<plan.ColumnSqlParameterValue>();
  readonly filterExpressions: SqlExpression<number | TableValuedFunctionOutput>[] = [];

  get tableValuedFunctions() {
    return [...this.mapper.tableValuedFunctions.values()];
  }

  constructor(source: plan.TableProcessor) {
    // Add table-valued functions and filters
    for (const fn of source.tableValuedFunctions) {
      const mapped: TableValuedFunction = {
        name: fn.functionName,
        inputs: fn.functionInputs.map((i) => this.mapper.transformWithoutTableValued(i))
      };
      this.mapper.tableValuedFunctions.set(fn, mapped);
    }

    for (const filter of source.filters) {
      this.filterExpressions.push(this.mapper.transform(filter));
    }
  }
}
