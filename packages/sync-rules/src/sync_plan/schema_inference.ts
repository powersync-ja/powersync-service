import { CompatibilityContext } from '../compatibility.js';
import { ColumnDefinition, ColumnType, ExpressionType } from '../ExpressionType.js';
import { generateSqlFunctions, getOperatorReturnType } from '../sql_functions.js';
import { SourceSchema, SourceSchemaTable } from '../types.js';
import {
  BinaryExpression,
  CaseWhenExpression,
  CastExpression,
  ExternalData,
  LiteralExpression,
  ScalarFunctionCallExpression,
  SqlExpression,
  UnaryExpression
} from './expression.js';
import { ExpressionVisitor, RecursiveExpressionVisitor, visitExpr } from './expression_visitor.js';
import {
  ColumnSqlParameterValue,
  ParameterValue,
  RequestSqlParameterValue,
  StreamDataSource,
  StreamQuerier,
  TableProcessor
} from './plan.js';

/**
 * Infers the output schema of sync streams by resolving references against a statically-known source schema.
 */
export class SyncPlanSchemaAnalyzer {
  constructor(
    private readonly defaultSchema: string,
    private readonly schema: SourceSchema
  ) {}

  /**
   * Populates an output record of tables with the inferred result sets for a stream data source against a source
   * schema.
   */
  resolveResultSets(source: StreamDataSource, tables: Record<string, Record<string, ColumnDefinition>>) {
    for (const table of this.schema.getTables(source.sourceTable.toTablePattern(this.defaultSchema))) {
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
            ...mergeType(existing, definition)
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

  /**
   * Resolves all parameters referenced in queriers for a stream, and attempts to infer their type.
   */
  resolveReferencedParameters(queriers: StreamQuerier[]): Record<string, ColumnType> {
    const parameters: Record<string, ColumnType> = {};
    const parameterInference = new ParameterTypeInference(parameters);

    const mergeTypes = (a: ColumnType[], b: ColumnType[]): ColumnType[] => {
      return a.map((type, index) => {
        const other = b[index];
        return mergeType(type, other);
      });
    };

    // Infers the type of parameters created by a bucket or parameter lookup.
    const inferSourceParameters = (sources: TableProcessor[]) => {
      let mergedTypes: ColumnType[] | null = null;
      for (const source of sources) {
        const tables = this.schema.getTables(source.sourceTable.toTablePattern(this.defaultSchema));

        for (const table of tables) {
          const typeResolver = new ExpressionTypeInference(table);
          const types = source.parameters.map((p) => visitExpr(typeResolver, p.expr, null));
          if (mergedTypes != null) {
            mergedTypes = mergeTypes(mergedTypes, types);
          } else {
            mergedTypes = types;
          }
        }
      }

      return mergedTypes!;
    };

    // If the given parameter value is a subscription parameter, infers its type to the expected type.
    const inferQuerierParameter = (expectedType: ColumnType, parameter: ParameterValue) => {
      switch (parameter.type) {
        case 'request':
          visitExpr(parameterInference, parameter.expr, expectedType);
          break;
        case 'lookup':
          const lookup = parameter.lookup;
          if (lookup.type === 'parameter') {
            const inputTypes = inferSourceParameters([lookup.lookup]);
            lookup.instantiation.forEach((param, i) => inferQuerierParameter(inputTypes[i], param));
          } else {
            // Inputs to table-valued functions must be strings.
            for (const input of lookup.functionInputs) {
              visitExpr(parameterInference, input, { type: ExpressionType.TEXT });
            }
          }
          break;
        case 'intersection':
          for (const value of parameter.values) {
            inferQuerierParameter(expectedType, value);
          }
          break;
      }
    };

    for (const querier of queriers) {
      // Infer types of bucket parameters to apply them to subscription parameters. For a stream defined as
      // `SELECT * FROM org WHERE id = subscription.parameter('org')`, this gives us [typeOfId]. By going through the
      // instantiation, we see that parameter 0 corresponds to `subscription.parameter('org')` and thus infer that org
      // needs to have a matching type. The same principle applies to values passed into parameter lookups.
      const parameterTypes = inferSourceParameters(querier.bucket.sources);
      querier.sourceInstantiation.forEach((param, i) => inferQuerierParameter(parameterTypes[i], param));

      for (const filter of querier.requestFilters) {
        visitExpr(parameterInference, filter, ExpressionTypeInference.BOOLEAN);
      }
    }

    return parameters;
  }
}

function mergeType(a: ColumnType, b: ColumnType): ColumnType {
  return {
    type: a.type.or(b.type),
    originalType: a.originalType === b.originalType ? a.originalType : undefined
  };
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
  static readonly BOOLEAN: ColumnType = { type: ExpressionType.INTEGER, originalType: 'bool' };
  static readonly ANY: ColumnType = { type: ExpressionType.ANY };
}

class ParameterTypeInference extends RecursiveExpressionVisitor<RequestSqlParameterValue, void, ColumnType> {
  constructor(readonly parameters: Record<string, ColumnType>) {
    super();
  }

  defaultExpression(expr: SqlExpression<RequestSqlParameterValue>, expectedType: ColumnType): void {
    // Recognize the "->>($subscription, $parameterName)" pattern.
    let foundParameter: string | null = null;
    if (expr.type == 'function' && expr.function == '->>') {
      const [source, key] = expr.parameters;
      if (source.type == 'data' && source.source.request === 'subscription' && key.type == 'lit_string') {
        foundParameter = key.value;
      }
    }

    if (foundParameter == null) {
      // Not a parameter, recurse into inner expressions. Because this might be a function call or another complex
      // expression, we can't pass the outer expectedType down without changes.
      return super.visitChildren(expr, ExpressionTypeInference.ANY);
    } else {
      const existing = this.parameters[foundParameter];
      this.parameters[foundParameter] = existing ? mergeType(existing, expectedType) : expectedType;
    }
  }
}
