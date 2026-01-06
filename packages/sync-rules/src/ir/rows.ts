import { EqualsIgnoringResultSet } from './compatibility.js';
import { RowExpression } from './filter.js';

export type ColumnSource = StarColumnSource | ExpressionColumnSource;

export class StarColumnSource implements EqualsIgnoringResultSet {
  private constructor() {}

  equalsAssumingSameResultSet(other: EqualsIgnoringResultSet): boolean {
    return other instanceof StarColumnSource;
  }

  assumingSameResultSetEqualityHashCode(): void {}

  static readonly instance = new StarColumnSource();
}

export class ExpressionColumnSource {
  constructor(
    readonly expression: RowExpression,
    readonly alias?: string
  ) {}
}
