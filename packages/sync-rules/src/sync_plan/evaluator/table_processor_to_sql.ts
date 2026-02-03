import {
  mapExternalDataToInstantiation,
  TableValuedFunction,
  TableValuedFunctionOutput
} from '../engine/scalar_expression_engine.js';
import { SqlExpression } from '../expression.js';
import * as plan from '../plan.js';

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

      for (const filter of fn.filters) {
        this.filterExpressions.push(this.mapper.transform(filter));
      }
    }

    for (const filter of source.filters) {
      this.filterExpressions.push(this.mapper.transform(filter));
    }
  }
}
