import { CompatibilityContext } from '../compatibility.js';
import { ColumnDefinition, ColumnType, ExpressionType } from '../ExpressionType.js';
import { generateSqlFunctions, getOperatorReturnType } from '../sql_functions.js';
import { SourceSchema, SourceSchemaTable } from '../types.js';
import {
  ExternalData,
  UnaryExpression,
  BinaryExpression,
  CaseWhenExpression,
  CastExpression,
  ScalarFunctionCallExpression,
  LiteralExpression
} from './expression.js';
import { ExpressionVisitor, visitExpr } from './expression_visitor.js';
import { ColumnSqlParameterValue, StreamDataSource } from './plan.js';

/**
 * Infers the output schema of sync streams by resolving references against a statically-known source schema.
 */
export class SyncPlanSchemaAnalyzer {
  constructor(private readonly schema: SourceSchema) {}

  /**
   * Populates an output record of tables with the inferred result sets for a stream data source against a source
   * schema.
   */
  resolveResultSets(source: StreamDataSource, tables: Record<string, Record<string, ColumnDefinition>>) {
    for (const table of this.schema.getTables(source.sourceTable)) {
      const typeResolver = new ExpressionTypeInference(table);
      const outputName = source.outputTableName ?? table.name;
      const outputTable = (tables[outputName] ??= {});

      function addOutputColumn(definition: ColumnDefinition) {
        if (definition.name == 'id') {
          return; // Is implicit
        }

        const existing = outputTable[definition.name];
        if (existing != null) {
          outputTable[definition.name] = {
            name: definition.name,
            type: existing.type.or(definition.type),
            originalType: definition.originalType == existing.originalType ? existing.originalType : undefined
          };
        } else {
          outputTable[definition.name] = definition;
        }
      }

      for (const column of source.columns) {
        if (column === 'star') {
          for (const actualColumn of table.getColumns()) {
            addOutputColumn(actualColumn);
          }
        } else {
          const type = visitExpr(typeResolver, column.expr, null);
          addOutputColumn({ name: column.alias, ...type });
        }
      }
    }
  }
}

/**
 * Infers the type of expressions, resolving column references against a fixed schema table.
 */
class ExpressionTypeInference implements ExpressionVisitor<ColumnSqlParameterValue, ColumnType> {
  constructor(private readonly sourceTable: SourceSchemaTable) {}

  visitExternalData(expr: ExternalData<ColumnSqlParameterValue>): ColumnType {
    const column = this.sourceTable.getColumn(expr.source.column);
    if (column) {
      return { type: column.type, originalType: column.originalType };
    }

    return { type: ExpressionType.NONE };
  }

  visitUnaryExpression(expr: UnaryExpression<ColumnSqlParameterValue>): ColumnType {
    switch (expr.operator) {
      case 'not':
        return ExpressionTypeInference.BOOLEAN;
      case '+':
        return visitExpr(this, expr.operand, null);
    }
  }

  visitBinaryExpression(expr: BinaryExpression<ColumnSqlParameterValue>): ColumnType {
    return {
      type: getOperatorReturnType(
        expr.operator.toUpperCase(),
        visitExpr(this, expr.left, null).type,
        visitExpr(this, expr.right, null).type
      )
    };
  }

  visitBetweenExpression(): ColumnType {
    return ExpressionTypeInference.BOOLEAN;
  }

  visitScalarInExpression(): ColumnType {
    return ExpressionTypeInference.BOOLEAN;
  }

  visitCaseWhenExpression(expr: CaseWhenExpression<ColumnSqlParameterValue>): ColumnType {
    let type = ExpressionType.NONE;
    // Create a union of all THEN expressions (and ELSE, if present).
    for (const { then } of expr.whens) {
      type = type.or(visitExpr(this, then, null).type);
    }
    if (expr.else) {
      type = type.or(visitExpr(this, expr.else, null).type);
    }

    return { type };
  }

  visitCastExpression(expr: CastExpression<ColumnSqlParameterValue>): ColumnType {
    return { type: ExpressionType.fromTypeText(expr.cast_as) };
  }

  visitScalarFunctionCallExpression(expr: ScalarFunctionCallExpression<ColumnSqlParameterValue>): ColumnType {
    const resolved = ExpressionTypeInference.functions.named[expr.function.toLowerCase()];
    const args = expr.parameters.map((p) => visitExpr(this, p, null).type);

    return { type: resolved.getReturnType(args) };
  }

  visitLiteralExpression(expr: LiteralExpression): ColumnType {
    switch (expr.type) {
      case 'lit_null':
        return { type: ExpressionType.NONE };
      case 'lit_double':
        return { type: ExpressionType.REAL };
      case 'lit_int':
        return { type: ExpressionType.INTEGER };
      case 'lit_string':
        return { type: ExpressionType.TEXT };
    }
  }

  // We don't care about compatibility as these functions are only used to infer types.
  private static readonly functions = generateSqlFunctions(CompatibilityContext.FULL_BACKWARDS_COMPATIBILITY);
  private static readonly BOOLEAN: ColumnType = { type: ExpressionType.INTEGER, originalType: 'bool' };
}
