import { isAny, SqliteValue, StaticFilter } from '@powersync/service-sync-rules';

export type MongoExpression =
  | Record<string, unknown>
  | unknown[]
  | string
  | number
  | boolean
  | null
  | bigint
  | Uint8Array;

export interface StaticFilterToMongoOptions {
  mapValue?: (value: SqliteValue) => unknown;
  parseJsonArrayForIn?: boolean;
  columnPrefix?: string;
}

export function staticFilterToMongoExpression(
  filter: StaticFilter,
  options: StaticFilterToMongoOptions = {}
): MongoExpression {
  const mapValue = options.mapValue ?? ((value: SqliteValue) => value);
  const parseJsonArrayForIn = options.parseJsonArrayForIn ?? true;
  // Marker for "cannot prefilter" so we avoid over-restricting the query.
  const ANY = Symbol('static-filter-any');

  const literalValue = (value: SqliteValue): MongoExpression => ({ $literal: mapValue(value) });

  const toExpr = (node: StaticFilter): MongoExpression | typeof ANY => {
    if (isAny(node)) {
      return ANY;
    }

    if ('and' in node) {
      const parts = node.and.map(toExpr).filter((part) => part !== ANY) as MongoExpression[];
      if (parts.length === 0) {
        return ANY;
      }
      if (parts.length === 1) {
        return parts[0];
      }
      return { $and: parts };
    }

    if ('or' in node) {
      const parts = node.or.map(toExpr);
      if (parts.some((part) => part === ANY)) {
        return ANY;
      }
      const expressions = parts.filter((part) => part !== ANY) as MongoExpression[];
      if (expressions.length === 0) {
        return { $const: false };
      }
      if (expressions.length === 1) {
        return expressions[0];
      }
      return { $or: expressions };
    }

    if ('operator' in node) {
      const left = toExpr(node.left);
      const right = toExpr(node.right);
      if (left === ANY || right === ANY) {
        return ANY;
      }

      const leftExpr = left as MongoExpression;
      let rightExpr = right as MongoExpression;
      const operator = node.operator.toUpperCase();

      switch (operator) {
        case '=':
        case '==':
        case 'IS':
          // Big hack to support boolean values coerced to integers.
          if ('value' in node.right && node.right.value == 0n) {
            return { $in: [leftExpr, [0n, false]] };
          } else if ('value' in node.right && node.right.value == 1n) {
            return { $in: [leftExpr, [1n, true]] };
          }
          return { $eq: [leftExpr, rightExpr] };
        case '!=':
        case '<>':
        case 'IS NOT':
          // Big hack to support boolean values coerced to integers.
          if ('value' in node.right && node.right.value == 0n) {
            return { $nin: [leftExpr, [0n, false]] };
          } else if ('value' in node.right && node.right.value == 1n) {
            return { $nin: [leftExpr, [1n, true]] };
          }
          return { $ne: [leftExpr, rightExpr] };
        case '<':
          return { $lt: [leftExpr, rightExpr] };
        case '<=':
          return { $lte: [leftExpr, rightExpr] };
        case '>':
          return { $gt: [leftExpr, rightExpr] };
        case '>=':
          return { $gte: [leftExpr, rightExpr] };
        case 'IN': {
          if (parseJsonArrayForIn && 'value' in node.right) {
            const value = node.right.value;
            if (typeof value !== 'string') {
              throw new Error('IN operator expects JSON array literal');
            }
            let parsed: unknown;
            try {
              parsed = JSON.parse(value);
            } catch {
              throw new Error('IN operator expects JSON array literal');
            }
            if (!Array.isArray(parsed)) {
              throw new Error('IN operator expects JSON array literal');
            }
            rightExpr = { $literal: parsed };
          }
          return { $in: [leftExpr, rightExpr] };
        }
        default:
          return ANY;
      }
    }

    if ('column' in node) {
      return `${options.columnPrefix ?? '$'}${node.column}`;
    }

    if ('value' in node) {
      return literalValue(node.value);
    }

    throw new Error('Unsupported static filter');
  };

  const result = toExpr(filter);
  return result === ANY ? { $literal: true } : result;
}
