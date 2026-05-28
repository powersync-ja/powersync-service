import { StableHasher } from '../compiler/equality.js';
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

  return a.hashCode == b.hashCode && arrayEquals(a.sources, b.sources, streamDataSourcesEqual);
}

export function hashStreamBucketDataSource(hasher: StableHasher, source: StreamBucketDataSource): void {
  hasher.addHash(source.hashCode);
  hashArray(hasher, source.sources, hashStreamDataSource);
}

function streamDataSourcesEqual(a: StreamDataSource, b: StreamDataSource): boolean {
  return (
    tableProcessorsEqual(a, b) &&
    a.outputTableName == b.outputTableName &&
    arrayEquals(a.columns, b.columns, (left, right) => columnSourcesEqual(left, right, a, b))
  );
}

function hashStreamDataSource(hasher: StableHasher, source: StreamDataSource): void {
  hashTableProcessor(hasher, source);
  hashOptionalString(hasher, source.outputTableName);
  hashArray(hasher, source.columns, (nested, column) => hashColumnSource(nested, column, source));
}

function tableProcessorsEqual(a: TableProcessor, b: TableProcessor): boolean {
  return (
    a.hashCode == b.hashCode &&
    a.sourceTable.equals(b.sourceTable) &&
    arrayEquals(a.tableValuedFunctions, b.tableValuedFunctions, tableValuedFunctionsEqual) &&
    arrayEquals(a.filters, b.filters, (left, right) => expressionsEqual(left, right, a, b)) &&
    arrayEquals(a.parameters, b.parameters, (left, right) => expressionsEqual(left.expr, right.expr, a, b))
  );
}

function hashTableProcessor(hasher: StableHasher, source: TableProcessor): void {
  hasher.addHash(source.hashCode);
  source.sourceTable.buildHash(hasher);
  hashArray(hasher, source.tableValuedFunctions, hashTableValuedFunction);
  hashArray(hasher, source.filters, (nested, expression) => hashExpression(nested, expression, source));
  hashArray(hasher, source.parameters, (nested, parameter) => hashExpression(nested, parameter.expr, source));
}

function tableValuedFunctionsEqual(
  a: TableProcessorTableValuedFunction,
  b: TableProcessorTableValuedFunction
): boolean {
  return (
    a.functionName == b.functionName && arrayEquals(a.functionInputs, b.functionInputs, columnParameterExpressionsEqual)
  );
}

function hashTableValuedFunction(hasher: StableHasher, fn: TableProcessorTableValuedFunction): void {
  hasher.addString(fn.functionName);
  hashArray(hasher, fn.functionInputs, hashColumnParameterExpression);
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

function columnParameterExpressionsEqual(
  a: SqlExpression<ColumnSqlParameterValue>,
  b: SqlExpression<ColumnSqlParameterValue>
): boolean {
  return (
    ExpressionToSqlite.toSqlite(a) == ExpressionToSqlite.toSqlite(b) && arrayEquals(columnInputs(a), columnInputs(b))
  );
}

function hashColumnParameterExpression(hasher: StableHasher, expression: SqlExpression<ColumnSqlParameterValue>): void {
  hasher.addString(ExpressionToSqlite.toSqlite(expression));
  hashArray(hasher, columnInputs(expression), (nested, input) => nested.addString(input));
}

function expressionsEqual(
  a: SqlExpression<TableProcessorData>,
  b: SqlExpression<TableProcessorData>,
  sourceA: TableProcessor,
  sourceB: TableProcessor
): boolean {
  return (
    ExpressionToSqlite.toSqlite(a) == ExpressionToSqlite.toSqlite(b) &&
    arrayEquals(expressionInputs(a, sourceA), expressionInputs(b, sourceB), planInputsEqual)
  );
}

function hashExpression(
  hasher: StableHasher,
  expression: SqlExpression<TableProcessorData>,
  source: TableProcessor
): void {
  hasher.addString(ExpressionToSqlite.toSqlite(expression));
  hashArray(hasher, expressionInputs(expression, source), hashPlanInput);
}

type PlanExpressionInput =
  | { type: 'column'; column: string }
  | { type: 'function'; functionIndex: number; outputName: string };

function planInputsEqual(a: PlanExpressionInput, b: PlanExpressionInput): boolean {
  if (a.type != b.type) {
    return false;
  }

  if (a.type == 'column') {
    return a.column == (b as typeof a).column;
  }

  return a.functionIndex == (b as typeof a).functionIndex && a.outputName == (b as typeof a).outputName;
}

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

function arrayEquals<T>(a: readonly T[], b: readonly T[]): boolean;
function arrayEquals<T>(a: readonly T[], b: readonly T[], equals: (a: T, b: T) => boolean): boolean;
function arrayEquals<T>(a: readonly T[], b: readonly T[], equals: (a: T, b: T) => boolean = Object.is): boolean {
  if (a.length != b.length) {
    return false;
  }

  return a.every((value, index) => equals(value, b[index]));
}

function hashArray<T>(hasher: StableHasher, values: readonly T[], hash: (hasher: StableHasher, value: T) => void) {
  hasher.addHash(values.length);
  for (const value of values) {
    hash(hasher, value);
  }
}

function hashOptionalString(hasher: StableHasher, value: string | undefined): void {
  if (value == null) {
    hasher.addHash(0);
  } else {
    hasher.addHash(1);
    hasher.addString(value);
  }
}
