import {
  BetweenExpression,
  BinaryExpression,
  BinaryOperator,
  CaseWhenExpression,
  CastExpression,
  ExternalData,
  LiteralExpression,
  ScalarFunctionCallExpression,
  ScalarInExpression,
  SqlExpression,
  UnaryExpression,
  UnaryOperator
} from './expression.js';
import { ExpressionVisitor, visitExpr } from './expression_visitor.js';

/**
 * Renders {@link SqlExpression}s into SQL text supported by SQLite.
 */
export class ExpressionToSqlite<Data> implements ExpressionVisitor<Data, void, Precedence | 0> {
  sql = '';

  private needsSpace = false;

  addExpression(expr: SqlExpression<Data>, precedence: Precedence | 0 = 0) {
    visitExpr(this, expr, precedence);
  }

  private space() {
    this.sql += ' ';
  }

  addLexeme(text: string, options?: { spaceLeft?: boolean; spaceRight?: boolean }): number {
    const spaceLeft = options?.spaceLeft ?? true;
    const spaceRight = options?.spaceRight ?? true;

    if (this.needsSpace && spaceLeft) {
      this.space();
    }

    const startOffset = this.sql.length;
    this.sql += text;
    this.needsSpace = spaceRight;
    return startOffset;
  }

  identifier(name: string) {
    this.addLexeme(`"${name.replaceAll('"', '""')}"`);
  }

  private string(name: string) {
    this.addLexeme(`'${name.replaceAll("'", "''")}'`);
  }

  private commaSeparated(exprList: Iterable<SqlExpression<Data>>) {
    let first = true;
    for (const expr of exprList) {
      if (!first) {
        this.addLexeme(',', { spaceLeft: false });
      }

      this.addExpression(expr);
      first = false;
    }
  }

  private parenthesis(inner: () => void) {
    this.addLexeme('(', { spaceRight: false });
    inner();
    this.addLexeme(')', { spaceLeft: false });
  }

  private maybeParenthesis(outerPrecedence: Precedence | 0, innerPrecedence: Precedence, inner: () => void) {
    if (outerPrecedence > innerPrecedence) {
      this.parenthesis(inner);
    } else {
      inner();
    }
  }

  visitExternalData(_expr: ExternalData<Data>): void {
    this.addLexeme('?');
  }

  visitUnaryExpression(expr: UnaryExpression<Data>, outerPrecedence: Precedence | 0): void {
    const innerPrecedence = unaryPrecedence[expr.operator];
    this.maybeParenthesis(outerPrecedence, innerPrecedence, () => {
      this.addLexeme(expr.operator);
      this.addExpression(expr.operand, innerPrecedence);
    });
  }

  visitBinaryExpression(expr: BinaryExpression<Data>, outerPrecedence: Precedence | 0): void {
    const innerPrecedence = binaryPrecedence[expr.operator];
    this.maybeParenthesis(outerPrecedence, innerPrecedence, () => {
      this.addExpression(expr.left, innerPrecedence);
      this.addLexeme(expr.operator);
      this.addExpression(expr.right, innerPrecedence);
    });
  }

  visitBetweenExpression(expr: BetweenExpression<Data>, outerPrecedence: 0 | Precedence): void {
    const innerPrecedence = Precedence.equals;
    this.maybeParenthesis(outerPrecedence, innerPrecedence, () => {
      this.addExpression(expr.value, Precedence.equals);
      this.addLexeme('BETWEEN');
      this.addExpression(expr.low, Precedence.equals);
      this.addLexeme('AND');
      this.addExpression(expr.high, Precedence.equals);
    });
  }

  visitScalarInExpression(expr: ScalarInExpression<Data>, arg: Precedence | 0): void {
    if (expr.in.length == 0) {
      // x IN () is invalid, but it can't be true either way.
      this.addLexeme('FALSE');
      return;
    }

    this.maybeParenthesis(arg, Precedence.equals, () => {
      this.addExpression(expr.target, Precedence.equals);
      this.addLexeme('IN');
      this.parenthesis(() => this.commaSeparated(expr.in));
    });
  }

  visitCaseWhenExpression(expr: CaseWhenExpression<Data>, arg: Precedence | 0): void {
    this.addLexeme('CASE');
    if (expr.operand) {
      this.addExpression(expr.operand);
    }
    for (const when of expr.whens) {
      this.addLexeme('WHEN');
      this.addExpression(when.when);
      this.addLexeme('THEN');
      this.addExpression(when.then);
    }

    if (expr.else) {
      this.addLexeme('ELSE');
      this.addExpression(expr.else);
    }
    this.addLexeme('END');
  }

  visitCastExpression(expr: CastExpression<Data>, arg: Precedence | 0): void {
    this.addLexeme('CAST(', { spaceRight: false });
    this.addExpression(expr.operand);
    this.addLexeme('AS');
    this.addLexeme(expr.cast_as);
    this.addLexeme(')', { spaceLeft: false });
  }

  visitScalarFunctionCallExpression(expr: ScalarFunctionCallExpression<Data>, arg: Precedence | 0): void {
    this.identifier(expr.function);
    this.addLexeme('(', { spaceLeft: false, spaceRight: false });
    this.commaSeparated(expr.parameters);
    this.addLexeme(')', { spaceLeft: false });
  }

  visitLiteralExpression(expr: LiteralExpression, arg: Precedence | 0): void {
    if (expr.type == 'lit_null') {
      this.addLexeme('NULL');
    } else if (expr.type == 'lit_double') {
      this.addLexeme(expr.value.toString());
    } else if (expr.type == 'lit_int') {
      this.addLexeme(expr.base10);
    } else {
      this.string(expr.value);
    }
  }

  static toSqlite(expr: SqlExpression<unknown>): string {
    const visitor = new ExpressionToSqlite<unknown>();
    visitor.addExpression(expr);
    return visitor.sql;
  }
}

enum Precedence {
  or = 1,
  and = 2,
  not = 3,
  equals = 4,
  comparison = 5,
  binary = 6,
  addition = 7,
  multiplication = 8,
  concat = 9,
  collate = 10,
  unary = 11
}

// https://www.sqlite.org/lang_expr.html#operators_and_parse_affecting_attributes
const binaryPrecedence: Record<BinaryOperator, Precedence> = {
  or: Precedence.or,
  and: Precedence.and,
  '=': Precedence.equals,
  is: Precedence.equals,
  '<': Precedence.comparison,
  '>': Precedence.comparison,
  '<=': Precedence.comparison,
  '>=': Precedence.comparison,
  '&': Precedence.binary,
  '|': Precedence.binary,
  '<<': Precedence.binary,
  '>>': Precedence.binary,
  '+': Precedence.addition,
  '-': Precedence.addition,
  '*': Precedence.multiplication,
  '/': Precedence.multiplication,
  '%': Precedence.multiplication,
  '||': Precedence.concat
};

const unaryPrecedence: Record<UnaryOperator, Precedence> = {
  not: Precedence.not,
  '~': Precedence.unary,
  '+': Precedence.unary,
  '-': Precedence.unary
};
