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

export function streamBucketDataSourcesEqual(a: StreamBucketDataSource, b: StreamBucketDataSource): boolean {
  if (a === b) {
    return true;
  }

  return a.hashCode == b.hashCode && streamDataSourceSetEquality.equals(a.sources, b.sources);
}

export function hashStreamBucketDataSource(hasher: StableHasher, source: StreamBucketDataSource): void {
  hasher.addHash(source.hashCode);
  streamDataSourceSetEquality.hash(hasher, source.sources);
}

const streamDataSourceEquality: Equality<StreamDataSource> = {
  equals: streamDataSourcesEqual,
  hash: hashStreamDataSource
};

const streamDataSourceSetEquality = unorderedEquality(streamDataSourceEquality);

function streamDataSourcesEqual(a: StreamDataSource, b: StreamDataSource): boolean {
  return (
    tableProcessorsEqual(a, b) &&
    a.outputTableName == b.outputTableName &&
    listEquals(a.columns, b.columns, (left, right) => columnSourcesEqual(left, right, a, b))
  );
}

function hashStreamDataSource(hasher: StableHasher, source: StreamDataSource): void {
  hashTableProcessor(hasher, source);
  hashOptionalString(hasher, source.outputTableName);
  hashList(hasher, source.columns, (nested, column) => hashColumnSource(nested, column, source));
}

function tableProcessorsEqual(a: TableProcessor, b: TableProcessor): boolean {
  return (
    a.hashCode == b.hashCode &&
    a.sourceTable.equals(b.sourceTable) &&
    tableValuedFunctionListEquality.equals(a.tableValuedFunctions, b.tableValuedFunctions) &&
    listEquals(a.filters, b.filters, (left, right) => expressionsEqual(left, right, a, b)) &&
    listEquals(a.parameters, b.parameters, (left, right) => expressionsEqual(left.expr, right.expr, a, b))
  );
}

function hashTableProcessor(hasher: StableHasher, source: TableProcessor): void {
  hasher.addHash(source.hashCode);
  source.sourceTable.buildHash(hasher);
  tableValuedFunctionListEquality.hash(hasher, source.tableValuedFunctions);
  hashList(hasher, source.filters, (nested, expression) => hashExpression(nested, expression, source));
  hashList(hasher, source.parameters, (nested, parameter) => hashExpression(nested, parameter.expr, source));
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

function columnSourcesEqual(
  a: ColumnSource,
  b: ColumnSource,
  sourceA: TableProcessor,
  sourceB: TableProcessor
): boolean {
  if (a == 'star' || b == 'star') {
    return a == b;
  }

  return a.alias == b.alias && expressionsEqual(a.expr, b.expr, sourceA, sourceB);
}

function hashColumnSource(hasher: StableHasher, column: ColumnSource, source: TableProcessor): void {
  if (column == 'star') {
    hasher.addHash(0);
  } else {
    hasher.addHash(1);
    hasher.addString(column.alias);
    hashExpression(hasher, column.expr, source);
  }
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

function listEquals<T>(a: readonly T[], b: readonly T[], equals: (a: T, b: T) => boolean): boolean {
  return listEquality({ equals, hash: () => {} }).equals(a, b);
}

function hashList<T>(hasher: StableHasher, values: readonly T[], hash: (hasher: StableHasher, value: T) => void) {
  listEquality({ equals: Object.is, hash }).hash(hasher, values);
}

function hashOptionalString(hasher: StableHasher, value: string | undefined): void {
  if (value == null) {
    hasher.addHash(0);
  } else {
    hasher.addHash(1);
    hasher.addString(value);
  }
}
