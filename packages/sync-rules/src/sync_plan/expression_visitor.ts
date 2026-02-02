import {
  BetweenExpression,
  BinaryExpression,
  CaseWhenExpression,
  CastExpression,
  ExternalData,
  LiteralExpression,
  ScalarFunctionCallExpression,
  ScalarInExpression,
  SqlExpression,
  UnaryExpression
} from './expression.js';

/**
 * Callbacks for each type of SQL expression we support.
 *
 * The {@link visit} function can be used to call the appropriate callback given an expression.
 */
export interface ExpressionVisitor<Data, R, Arg = undefined> {
  visitExternalData(expr: ExternalData<Data>, arg: Arg): R;
  visitUnaryExpression(expr: UnaryExpression<Data>, arg: Arg): R;
  visitBinaryExpression(expr: BinaryExpression<Data>, arg: Arg): R;
  visitBetweenExpression(expr: BetweenExpression<Data>, arg: Arg): R;
  visitScalarInExpression(expr: ScalarInExpression<Data>, arg: Arg): R;
  visitCaseWhenExpression(expr: CaseWhenExpression<Data>, arg: Arg): R;
  visitCastExpression(expr: CastExpression<Data>, arg: Arg): R;
  visitScalarFunctionCallExpression(expr: ScalarFunctionCallExpression<Data>, arg: Arg): R;
  visitLiteralExpression(expr: LiteralExpression, arg: Arg): R;
}

/**
 * Invokes the appropriate visit method for the given expression.
 */
export function visitExpr<Data, R, Arg>(
  visitor: ExpressionVisitor<Data, R, Arg>,
  expr: SqlExpression<Data>,
  arg: Arg
): R {
  switch (expr.type) {
    case 'data':
      return visitor.visitExternalData(expr, arg);
    case 'unary':
      return visitor.visitUnaryExpression(expr, arg);
    case 'between':
      return visitor.visitBetweenExpression(expr, arg);
    case 'binary':
      return visitor.visitBinaryExpression(expr, arg);
    case 'scalar_in':
      return visitor.visitScalarInExpression(expr, arg);
    case 'case_when':
      return visitor.visitCaseWhenExpression(expr, arg);
    case 'cast':
      return visitor.visitCastExpression(expr, arg);
    case 'function':
      return visitor.visitScalarFunctionCallExpression(expr, arg);
    default:
      return visitor.visitLiteralExpression(expr, arg);
  }
}

/**
 * A utility for traversing through a {@link SqlExpression} tree.
 */
export abstract class RecursiveExpressionVisitor<Data, R, Arg = undefined> implements ExpressionVisitor<Data, R, Arg> {
  abstract defaultExpression(expr: SqlExpression<Data>, arg: Arg): R;

  /**
   * Invokes the appropriate visit method for the given expression.
   */
  visit(expr: SqlExpression<Data>, arg: Arg): R {
    return visitExpr(this, expr, arg);
  }

  visitChildren(expr: SqlExpression<Data>, arg: Arg) {
    switch (expr.type) {
      case 'data':
        break; // No subexpression
      case 'unary':
        this.visit(expr.operand, arg);
        break;
      case 'between':
        this.visit(expr.value, arg);
        this.visit(expr.low, arg);
        this.visit(expr.high, arg);
        break;
      case 'binary':
        this.visit(expr.left, arg);
        this.visit(expr.right, arg);
        break;
      case 'scalar_in':
        this.visit(expr.target, arg);
        for (const target of expr.in) {
          this.visit(target, arg);
        }
        break;
      case 'case_when':
        return this.visitCaseWhenExpression(expr, arg);
      case 'cast':
        this.visit(expr.operand, arg);
        break;
      case 'function':
        for (const param of expr.parameters) {
          this.visit(param, arg);
        }
        break;
      default:
      // Literals have no subexpressions.
    }
  }

  visitExternalData(expr: ExternalData<Data>, arg: Arg) {
    return this.defaultExpression(expr, arg);
  }

  visitUnaryExpression(expr: UnaryExpression<Data>, arg: Arg) {
    return this.defaultExpression(expr, arg);
  }

  visitBinaryExpression(expr: BinaryExpression<Data>, arg: Arg) {
    return this.defaultExpression(expr, arg);
  }

  visitBetweenExpression(expr: BetweenExpression<Data>, arg: Arg) {
    return this.defaultExpression(expr, arg);
  }

  visitScalarInExpression(expr: ScalarInExpression<Data>, arg: Arg) {
    return this.defaultExpression(expr, arg);
  }

  visitCaseWhenExpression(expr: CaseWhenExpression<Data>, arg: Arg) {
    return this.defaultExpression(expr, arg);
  }

  visitCastExpression(expr: CastExpression<Data>, arg: Arg) {
    return this.defaultExpression(expr, arg);
  }

  visitScalarFunctionCallExpression(expr: ScalarFunctionCallExpression<Data>, arg: Arg) {
    return this.defaultExpression(expr, arg);
  }

  visitLiteralExpression(expr: LiteralExpression, arg: Arg) {
    return this.defaultExpression(expr, arg);
  }
}

/**
 * A visitor applying a mapping function to external data references in expressions.
 */
export class MapSourceVisitor<DataIn, DataOut> implements ExpressionVisitor<DataIn, SqlExpression<DataOut>> {
  constructor(private readonly map: (a: DataIn) => DataOut) {}

  visitExternalData(expr: ExternalData<DataIn>): SqlExpression<DataOut> {
    return { type: 'data', source: this.map(expr.source) };
  }

  visitUnaryExpression(expr: UnaryExpression<DataIn>, arg: undefined): SqlExpression<DataOut> {
    const operand = visitExpr(this, expr.operand, arg);
    if (operand == expr.operand) {
      return expr as SqlExpression<DataOut>; // unchanged subtree
    }

    return { type: 'unary', operator: expr.operator, operand };
  }

  visitBinaryExpression(expr: BinaryExpression<DataIn>, arg: undefined): SqlExpression<DataOut> {
    const left = visitExpr(this, expr.left, arg);
    const right = visitExpr(this, expr.right, arg);
    if (left == expr.left && right == expr.right) {
      return expr as SqlExpression<DataOut>; // unchanged subtree
    }

    return { type: 'binary', operator: expr.operator, left, right };
  }

  visitBetweenExpression(expr: BetweenExpression<DataIn>, arg: undefined): SqlExpression<DataOut> {
    const value = visitExpr(this, expr.value, arg);
    const low = visitExpr(this, expr.low, arg);
    const high = visitExpr(this, expr.high, arg);
    if (value == expr.value && expr.low == low && expr.high == high) {
      return expr as SqlExpression<DataOut>; // unchanged subtree
    }

    return { type: 'between', value, low, high };
  }

  visitScalarInExpression(expr: ScalarInExpression<DataIn>, arg: undefined): SqlExpression<DataOut> {
    return {
      type: 'scalar_in',
      target: visitExpr(this, expr.target, arg),
      in: expr.in.map((e) => visitExpr(this, e, arg))
    };
  }

  visitCaseWhenExpression(expr: CaseWhenExpression<DataIn>, arg: undefined): SqlExpression<DataOut> {
    return {
      type: 'case_when',
      operand: expr.operand && visitExpr(this, expr.operand, arg),
      whens: expr.whens.map(({ when, then }) => ({
        when: visitExpr(this, when, arg),
        then: visitExpr(this, then, arg)
      })),
      else: expr.else && visitExpr(this, expr.else, arg)
    };
  }

  visitCastExpression(expr: CastExpression<DataIn>, arg: undefined): SqlExpression<DataOut> {
    const operand = visitExpr(this, expr.operand, arg);
    if (operand == expr.operand) {
      return expr as SqlExpression<DataOut>; // unchanged subtree
    }

    return { type: 'cast', operand, cast_as: expr.cast_as };
  }

  visitScalarFunctionCallExpression(
    expr: ScalarFunctionCallExpression<DataIn>,
    arg: undefined
  ): SqlExpression<DataOut> {
    return {
      type: 'function',
      function: expr.function,
      parameters: expr.parameters.map((p) => visitExpr(this, p, arg))
    };
  }

  visitLiteralExpression(expr: LiteralExpression, arg: undefined): SqlExpression<DataOut> {
    return expr;
  }
}
