import { Equality, listEquality, StableHasher, unorderedEquality } from '../compiler/equality.js';
import { SqlExpression } from './expression.js';
import { ExpressionToSqlite } from './expression_to_sql.js';
import { RecursiveExpressionVisitor } from './expression_visitor.js';
import {
  ColumnSource,
  ColumnSqlParameterValue,
  StreamBucketDataSource,
  StreamDataSource,
  TableProcessor,
  TableProcessorData,
  TableProcessorTableValuedFunction,
  TableProcessorTableValuedFunctionOutput
} from './plan.js';

export const streamBucketDataSourceEquality: Equality<StreamBucketDataSource> = {
  hash(hasher, value) {
    hasher.addHash(value.hashCode);
    hasher.addString(value.uniqueName);
  },
  equals(a, b) {
    return (
      a === b ||
      (a.hashCode == b.hashCode &&
        a.uniqueName == b.uniqueName &&
        streamDataSourceSetEquality.equals(a.sources, b.sources))
    );
  }
};

export const streamDataSourceEquality: Equality<StreamDataSource> = {
  hash(hasher, value) {
    hasher.addHash(value.hashCode);
  },
  equals(a, b) {
    return (
      a === b ||
      (tableProcessorsEqual(a, b) &&
        a.outputTableName == b.outputTableName &&
        columnSourceListEquality(a, b).equals(a.columns, b.columns))
    );
  }
};

const streamDataSourceSetEquality = unorderedEquality(streamDataSourceEquality);

function tableProcessorsEqual(a: TableProcessor, b: TableProcessor): boolean {
  return (
    a.hashCode == b.hashCode &&
    a.sourceTable.equals(b.sourceTable) &&
    tableValuedFunctionListEquality.equals(a.tableValuedFunctions, b.tableValuedFunctions) &&
    expressionListEquality(a, b).equals(a.filters, b.filters) &&
    partitionKeyListEquality(a, b).equals(a.parameters, b.parameters)
  );
}

const tableValuedFunctionEquality: Equality<TableProcessorTableValuedFunction> = {
  equals(a, b) {
    return (
      a.functionName == b.functionName &&
      columnParameterExpressionListEquality.equals(a.functionInputs, b.functionInputs)
    );
  },
  hash: hashTableValuedFunction
};

const tableValuedFunctionListEquality = listEquality(tableValuedFunctionEquality);

function hashTableValuedFunction(hasher: StableHasher, fn: TableProcessorTableValuedFunction): void {
  hasher.addString(fn.functionName);
  columnParameterExpressionListEquality.hash(hasher, fn.functionInputs);
}

function columnSourceEquality(sourceA: TableProcessor, sourceB: TableProcessor): Equality<ColumnSource> {
  return {
    hash(hasher, value) {
      if (value == 'star') {
        hasher.addHash(0);
      } else {
        hasher.addHash(1);
        hasher.addString(value.alias);
        hashExpression(hasher, value.expr, sourceA);
      }
    },
    equals(a, b) {
      if (a == 'star' || b == 'star') {
        return a == b;
      }

      return a.alias == b.alias && expressionsEqual(a.expr, b.expr, sourceA, sourceB);
    }
  };
}

function columnSourceListEquality(sourceA: TableProcessor, sourceB: TableProcessor): Equality<Iterable<ColumnSource>> {
  return listEquality(columnSourceEquality(sourceA, sourceB));
}

const columnParameterExpressionEquality: Equality<SqlExpression<ColumnSqlParameterValue>> = {
  equals(a, b) {
    return (
      ExpressionToSqlite.toSqlite(a) == ExpressionToSqlite.toSqlite(b) &&
      stringListEquality.equals(columnInputs(a), columnInputs(b))
    );
  },
  hash: hashColumnParameterExpression
};

const columnParameterExpressionListEquality = listEquality(columnParameterExpressionEquality);

function hashColumnParameterExpression(hasher: StableHasher, expression: SqlExpression<ColumnSqlParameterValue>): void {
  hasher.addString(ExpressionToSqlite.toSqlite(expression));
  stringListEquality.hash(hasher, columnInputs(expression));
}

function expressionsEqual(
  a: SqlExpression<TableProcessorData>,
  b: SqlExpression<TableProcessorData>,
  sourceA: TableProcessor,
  sourceB: TableProcessor
): boolean {
  return (
    ExpressionToSqlite.toSqlite(a) == ExpressionToSqlite.toSqlite(b) &&
    planInputListEquality.equals(expressionInputs(a, sourceA), expressionInputs(b, sourceB))
  );
}

function hashExpression(
  hasher: StableHasher,
  expression: SqlExpression<TableProcessorData>,
  source: TableProcessor
): void {
  hasher.addString(ExpressionToSqlite.toSqlite(expression));
  planInputListEquality.hash(hasher, expressionInputs(expression, source));
}

function expressionEquality(
  sourceA: TableProcessor,
  sourceB: TableProcessor
): Equality<SqlExpression<TableProcessorData>> {
  return {
    hash(hasher, value) {
      hashExpression(hasher, value, sourceA);
    },
    equals(a, b) {
      return expressionsEqual(a, b, sourceA, sourceB);
    }
  };
}

function expressionListEquality(
  sourceA: TableProcessor,
  sourceB: TableProcessor
): Equality<Iterable<SqlExpression<TableProcessorData>>> {
  return listEquality(expressionEquality(sourceA, sourceB));
}

function partitionKeyListEquality(sourceA: TableProcessor, sourceB: TableProcessor) {
  return listEquality({
    hash(hasher, value: { expr: SqlExpression<TableProcessorData> }) {
      hashExpression(hasher, value.expr, sourceA);
    },
    equals(a: { expr: SqlExpression<TableProcessorData> }, b: { expr: SqlExpression<TableProcessorData> }) {
      return expressionsEqual(a.expr, b.expr, sourceA, sourceB);
    }
  });
}

type PlanExpressionInput =
  | { type: 'column'; column: string }
  | { type: 'function'; functionIndex: number; outputName: string };

const planInputEquality: Equality<PlanExpressionInput> = {
  equals(a, b) {
    if (a.type != b.type) {
      return false;
    }

    if (a.type == 'column') {
      return a.column == (b as typeof a).column;
    }

    return a.functionIndex == (b as typeof a).functionIndex && a.outputName == (b as typeof a).outputName;
  },
  hash: hashPlanInput
};

const planInputListEquality = listEquality(planInputEquality);

const stringEquality: Equality<string> = {
  equals(a, b) {
    return a == b;
  },
  hash(hasher, value) {
    hasher.addString(value);
  }
};

const stringListEquality = listEquality(stringEquality);

function hashPlanInput(hasher: StableHasher, input: PlanExpressionInput): void {
  if (input.type == 'column') {
    hasher.addHash(0);
    hasher.addString(input.column);
  } else {
    hasher.addHash(1);
    hasher.addHash(input.functionIndex);
    hasher.addString(input.outputName);
  }
}

function expressionInputs(
  expression: SqlExpression<TableProcessorData>,
  source: TableProcessor
): PlanExpressionInput[] {
  const inputs: PlanExpressionInput[] = [];
  FindTableProcessorInputs.instance.visit(expression, { processor: source, inputs });
  return inputs;
}

function columnInputs(expression: SqlExpression<ColumnSqlParameterValue>): string[] {
  const inputs: string[] = [];
  FindColumnInputs.instance.visit(expression, inputs);
  return inputs;
}

class FindTableProcessorInputs extends RecursiveExpressionVisitor<
  TableProcessorData,
  void,
  { processor: TableProcessor; inputs: PlanExpressionInput[] }
> {
  defaultExpression(
    expr: SqlExpression<TableProcessorData>,
    arg: { processor: TableProcessor; inputs: PlanExpressionInput[] }
  ) {
    this.visitChildren(expr, arg);
  }

  visitExternalData(
    expr: { type: 'data'; source: TableProcessorData },
    arg: { processor: TableProcessor; inputs: PlanExpressionInput[] }
  ) {
    const source = expr.source;
    if (isTableValuedFunctionOutput(source)) {
      arg.inputs.push({
        type: 'function',
        functionIndex: arg.processor.tableValuedFunctions.indexOf(source.function),
        outputName: source.outputName
      });
    } else {
      arg.inputs.push({ type: 'column', column: source.column });
    }
  }

  static readonly instance = new FindTableProcessorInputs();
}

class FindColumnInputs extends RecursiveExpressionVisitor<ColumnSqlParameterValue, void, string[]> {
  defaultExpression(expr: SqlExpression<ColumnSqlParameterValue>, arg: string[]) {
    this.visitChildren(expr, arg);
  }

  visitExternalData(expr: { type: 'data'; source: ColumnSqlParameterValue }, arg: string[]) {
    arg.push(expr.source.column);
  }

  static readonly instance = new FindColumnInputs();
}

function isTableValuedFunctionOutput(value: TableProcessorData): value is TableProcessorTableValuedFunctionOutput {
  return 'function' in value;
}
