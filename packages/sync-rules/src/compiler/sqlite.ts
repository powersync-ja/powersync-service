import { BinaryOperator, Expr, ExprCall, UnaryOperator } from 'pgsql-ast-parser';
import { ParsingErrorListener } from './compiler.js';
import { CAST_TYPES } from '../sql_functions.js';
import { ExpressionInput, ExpressionInputWithSpan } from './expression.js';

export const intrinsicContains = 'intrinsic:contains';

/**
 * Utility for translating Postgres expressions to SQLite expressions.
 *
 * Unsupported Postgres features are reported as errors.
 */
export class PostgresToSqlite {
  sql = '';
  inputs: ExpressionInputWithSpan[] = [];

  private needsSpace = false;

  constructor(
    private readonly originalSource: string,
    private readonly errors: ParsingErrorListener,
    private readonly parameters: ExpressionInput[]
  ) {}

  private space() {
    this.sql += ' ';
  }

  private addLexeme(text: string, options?: { spaceLeft?: boolean; spaceRight?: boolean }): number {
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

  private identifier(name: string) {
    this.addLexeme(`"${name.replaceAll('"', '""')}"`);
  }

  private string(name: string) {
    this.addLexeme(`'${name.replaceAll("'", "''")}'`);
  }

  private bogusExpression() {
    this.addLexeme('NULL');
  }

  private commaSeparated(exprList: Iterable<Expr>) {
    let first = true;
    for (const expr of exprList) {
      if (!first) {
        this.addLexeme(',', { spaceLeft: false });
      }

      this.addExpression(expr);
      first = false;
    }
  }

  private intrinsicContains(call: ExprCall, outerPrecedence: Precedence | 0) {
    const [left, ...right] = call.args;
    if (right.length == 0) {
      this.addLexeme('FALSE');
      return;
    }

    this.maybeParenthesis(outerPrecedence, Precedence.equals, () => {
      this.addExpression(left, Precedence.equals);
      this.addLexeme('IN');
      this.parenthesis(() => this.commaSeparated(right));
    });
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

  addExpression(expr: Expr, outerPrecedence: Precedence | 0 = 0) {
    // export type Expr =  |  ExprExtract | ExprMember  ;
    switch (expr.type) {
      case 'null':
        this.addLexeme('NULL');
        break;
      case 'boolean':
        this.addLexeme(expr.value ? 'TRUE' : 'FALSE');
        break;
      case 'string':
        this.string(expr.value);
        break;
      case 'numeric':
      case 'integer': {
        // JavaScript does not have a number type that can represent SQLite values, so we try to reuse the source.
        if (expr._location) {
          this.addLexeme(this.originalSource.substring(expr._location.start, expr._location.end));
        } else {
          this.addLexeme(expr.value.toString());
        }
        break;
      }
      case 'ref':
        this.bogusExpression();
        this.errors.report('Internal error: Dependency should have been extracted', expr);
        break;
      case 'parameter':
        // The name is going to be ?<index>
        const index = Number(expr.name.substring(1));
        const value = this.parameters[index - 1];
        if (value == null) {
          throw new Error('Internal error: No value given for parameter');
        }
        const start = this.addLexeme('?');
        this.inputs.push(new ExpressionInputWithSpan(value, start, 1));
        break;
      case 'substring': {
        const args = [expr.value, expr.from ?? { type: 'numeric', value: 1 }];
        if (expr.for != null) {
          args.push(expr.for);
        }
        this.addExpression({
          type: 'call',
          function: { name: 'substr' },
          args
        });
        break;
      }
      case 'call': {
        if (expr.function.name == intrinsicContains) {
          // Calls to this function should be lowered to a IN (x, y, z). We can't represent that in Postgres AST.
          return this.intrinsicContains(expr, outerPrecedence);
        }

        if (expr.function.schema) {
          this.errors.report('Invalid schema in function name', expr.function);
          return this.bogusExpression();
        }
        if (expr.distinct != null || expr.orderBy != null || expr.filter != null || expr.over != null) {
          this.errors.report('DISTINCT, ORDER BY, FILTER and OVER clauses are not supported', expr.function);
          return this.bogusExpression();
        }

        const forbiddenReason = forbiddenFunctions[expr.function.name];
        if (forbiddenReason) {
          this.errors.report(`Forbidden call: ${forbiddenReason}`, expr.function);
          return this.bogusExpression();
        }
        let allowedArgs = supportedFunctions[expr.function.name];
        if (allowedArgs == null) {
          this.errors.report('Unknown function', expr.function);
          return this.bogusExpression();
        } else {
          if (typeof allowedArgs == 'number') {
            allowedArgs = { min: allowedArgs, max: allowedArgs };
          }

          const actualArgs = expr.args.length;
          if (actualArgs < allowedArgs.min) {
            this.errors.report(`Expected at least ${allowedArgs.min} arguments`, expr);
          } else if (allowedArgs.max && actualArgs > allowedArgs.max) {
            this.errors.report(`Expected at most ${allowedArgs.max} arguments`, expr);
          } else if (allowedArgs.mustBeEven && actualArgs % 2 == 1) {
            this.errors.report(`Expected an even amount of arguments`, expr);
          } else if (allowedArgs.mustBeOdd && actualArgs % 2 == 0) {
            this.errors.report(`Expected an odd amount of arguments`, expr);
          }
        }

        this.identifier(expr.function.name);
        this.addLexeme('(', { spaceLeft: false, spaceRight: false });
        this.commaSeparated(expr.args);
        this.addLexeme(')', { spaceLeft: false });
        break;
      }
      case 'binary': {
        const precedence = supportedBinaryOperators[expr.op];
        if (precedence == null) {
          this.bogusExpression();
          this.errors.report('Unsupported binary operator', expr);
        } else {
          this.maybeParenthesis(outerPrecedence, precedence, () => {
            this.addExpression(expr.left, precedence);
            this.addLexeme(expr.op);
            this.addExpression(expr.right, precedence);
          });
        }
        break;
      }
      case 'unary': {
        const supported = supportedUnaryOperators[expr.op];
        if (supported == null) {
          this.errors.report('Unsupported unary operator', expr);
          return this.bogusExpression();
        }

        const [isSuffix, precedence] = supported;
        this.maybeParenthesis(outerPrecedence, precedence, () => {
          if (!isSuffix) this.addLexeme(expr.op);
          this.addExpression(expr.operand, precedence);
          if (isSuffix) this.addLexeme(expr.op);
        });
        break;
      }
      case 'cast': {
        const to = (expr.to as any)?.name?.toLowerCase() as string | undefined;
        if (to == null || !CAST_TYPES.has(to)) {
          this.errors.report('Invalid SQLite cast', expr.to);
          return this.bogusExpression();
        } else {
          this.addLexeme('CAST(', { spaceRight: false });
          this.addExpression(expr.operand);
          this.addLexeme('AS');
          this.addLexeme(to);
          this.addLexeme(')', { spaceLeft: false });
        }

        break;
      }
      case 'ternary': {
        this.maybeParenthesis(outerPrecedence, Precedence.equals, () => {
          this.addExpression(expr.value, Precedence.equals);
          this.addLexeme(expr.op);
          this.addExpression(expr.lo, Precedence.equals);
          this.addLexeme('AND');
          this.addExpression(expr.hi, Precedence.equals);
        });
        break;
      }
      case 'case': {
        this.addLexeme('CASE');
        if (expr.value) {
          this.addExpression(expr.value);
        }
        for (const when of expr.whens) {
          this.addLexeme('WHEN');
          this.addExpression(when.when);
          this.addLexeme('THEN');
          this.addExpression(when.value);
        }

        if (expr.else) {
          this.addLexeme('ELSE');
          this.addExpression(expr.else);
        }
        this.addLexeme('END');
        break;
      }
      case 'member': {
        this.maybeParenthesis(outerPrecedence, Precedence.concat, () => {
          this.addExpression(expr.operand, Precedence.concat);
          this.addLexeme(expr.op);
          if (typeof expr.member == 'number') {
            this.addExpression({ type: 'integer', value: expr.member });
          } else {
            this.addExpression({ type: 'string', value: expr.member });
          }
        });
        break;
      }
      case 'select':
      case 'union':
      case 'union all':
      case 'with':
      case 'with recursive':
        // Should have been desugared.
        this.bogusExpression();
        this.errors.report('Invalid position for subqueries. Subqueries are only supported in WHERE clauses.', expr);
        break;
      default:
        this.bogusExpression();
        this.errors.report('This expression is not supported by PowerSync', expr);
    }
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

type ArgumentCount = number | { min: number; max?: number; mustBeEven?: boolean; mustBeOdd?: boolean };

const supportedFunctions: Record<string, ArgumentCount> = {
  // https://sqlite.org/lang_corefunc.html#list_of_core_functions
  abs: 1,
  char: { min: 0 },
  coalesce: { min: 2 },
  concat: { min: 1 },
  concat_ws: { min: 2 },
  format: { min: 1 },
  glob: 2,
  hex: 1,
  ifnull: { min: 2 },
  if: { min: 2 },
  iif: { min: 2 },
  instr: 2,
  length: 1,
  like: { min: 2, max: 3 },
  likelihood: 2,
  likely: 1,
  lower: 1,
  ltrim: { min: 1, max: 2 },
  max: { min: 2 },
  min: { min: 2 },
  nullif: 2,
  octet_length: 1,
  printf: { min: 1 },
  quote: 1,
  replace: 3,
  round: { min: 1, max: 2 },
  rtrim: { min: 1, max: 2 },
  sign: 1,
  substr: { min: 2, max: 3 },
  substring: { min: 2, max: 3 },
  trim: { min: 1, max: 2 },
  typeof: 1,
  unhex: { min: 1, max: 2 },
  unicode: 1,
  unistr: 1,
  unistr_quote: 1,
  unlikely: 1,
  upper: 1,
  zeroblob: 1,
  // Scalar functions from https://sqlite.org/json1.html#overview
  json: 1,
  jsonb: 1,
  json_array: { min: 0 },
  jsonb_array: { min: 0 },
  json_array_length: { min: 1, max: 2 },
  json_error_position: 1,
  json_extract: { min: 2 },
  jsonb_extract: { min: 2 },
  json_insert: { min: 3, mustBeOdd: true },
  jsonb_insert: { min: 3, mustBeOdd: true },
  json_object: { min: 0, mustBeEven: true },
  jsonb_object: { min: 0, mustBeEven: true },
  json_patch: 2,
  jsonb_patch: 2,
  json_pretty: 1,
  json_remove: { min: 2 },
  jsonb_remove: { min: 2 },
  json_replace: { min: 3, mustBeOdd: true },
  jsonb_replace: { min: 3, mustBeOdd: true },
  json_set: { min: 3, mustBeOdd: true },
  jsonb_set: { min: 3, mustBeOdd: true },
  json_type: { min: 1, max: 2 },
  json_valid: { min: 1, max: 2 },
  json_quote: { min: 1 }
};

const supportedBinaryOperators: Partial<Record<BinaryOperator, Precedence>> = {
  OR: Precedence.or,
  AND: Precedence.and,
  '=': Precedence.equals,
  '!=': Precedence.equals,
  LIKE: Precedence.equals,
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
  '||': Precedence.concat,
  ['->' as BinaryOperator]: Precedence.concat,
  ['->>' as BinaryOperator]: Precedence.concat
};

const supportedUnaryOperators: Partial<Record<UnaryOperator, [boolean, Precedence]>> = {
  NOT: [false, Precedence.not],
  'IS NULL': [true, Precedence.equals],
  'IS NOT NULL': [true, Precedence.equals],
  'IS FALSE': [true, Precedence.equals],
  'IS NOT FALSE': [true, Precedence.equals],
  'IS TRUE': [true, Precedence.equals],
  'IS NOT TRUE': [true, Precedence.equals],
  '+': [false, Precedence.unary],
  '-': [false, Precedence.unary]
};

const forbiddenFunctions: Record<string, string> = {
  random: 'Sync definitions must be deterministic.',
  randomBlob: 'Sync definitions must be deterministic.'
};
