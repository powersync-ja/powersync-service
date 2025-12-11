import { describe, expect, test } from 'vitest';
import { CompatibilityContext, DateTimeValue, TimeValuePrecision } from '../../../src/index.js';

describe('date formatting', () => {
  describe('default precision', () => {
    const context = new CompatibilityContext({ edition: 2 });

    test('mysql', () => {
      // MySQL provides six digits of precision, but should only emit three by default for backwards compatibility.
      expect(
        new DateTimeValue('2025-11-07T09:31:12.123456', undefined, mysqlOptions).toSqliteValue(context)
      ).toStrictEqual('2025-11-07T09:31:12.123');
    });

    test('postgres', () => {
      expect(
        new DateTimeValue('2025-11-07T09:31:12.123456', undefined, postgresOptions).toSqliteValue(context)
      ).toStrictEqual('2025-11-07T09:31:12.123456');
    });

    test('mongo', () => {
      expect(
        new DateTimeValue('2025-11-07T09:31:12.123', undefined, mongoOptions).toSqliteValue(context)
      ).toStrictEqual('2025-11-07T09:31:12.123');
    });
  });

  test('higher max precision than source', () => {
    const context = new CompatibilityContext({ edition: 2, maxTimeValuePrecision: TimeValuePrecision.microseconds });

    expect(new DateTimeValue('2025-11-07T09:31:12.123', undefined, mongoOptions).toSqliteValue(context)).toStrictEqual(
      '2025-11-07T09:31:12.123'
    );

    expect(new DateTimeValue('2025-11-07T09:31:12', undefined, mongoOptions).toSqliteValue(context)).toStrictEqual(
      '2025-11-07T09:31:12.000'
    );
  });

  test('enable higher precision for mysql', () => {
    const context = new CompatibilityContext({ edition: 2, maxTimeValuePrecision: TimeValuePrecision.microseconds });

    expect(
      new DateTimeValue('2025-11-07T09:31:12.123456', undefined, mysqlOptions).toSqliteValue(context)
    ).toStrictEqual('2025-11-07T09:31:12.123456');
  });

  test('reduce max precision to millis', () => {
    const context = new CompatibilityContext({ edition: 2, maxTimeValuePrecision: TimeValuePrecision.milliseconds });

    expect(
      new DateTimeValue('2025-11-07T09:31:12.123456', undefined, postgresOptions).toSqliteValue(context)
    ).toStrictEqual('2025-11-07T09:31:12.123');
    expect(
      new DateTimeValue('2025-11-07T09:31:12.123456Z', undefined, postgresOptions).toSqliteValue(context)
    ).toStrictEqual('2025-11-07T09:31:12.123Z');
  });

  test('reduce max precision to seconds', () => {
    const context = new CompatibilityContext({ edition: 2, maxTimeValuePrecision: TimeValuePrecision.seconds });

    expect(
      new DateTimeValue('2025-11-07T09:31:12.123456', undefined, postgresOptions).toSqliteValue(context)
    ).toStrictEqual('2025-11-07T09:31:12');
    expect(
      new DateTimeValue('2025-11-07T09:31:12.123456Z', undefined, postgresOptions).toSqliteValue(context)
    ).toStrictEqual('2025-11-07T09:31:12Z');

    expect(new DateTimeValue('2025-11-07T09:31:12.123', undefined, mongoOptions).toSqliteValue(context)).toStrictEqual(
      '2025-11-07T09:31:12'
    );
  });
});

const mysqlOptions = {
  subSecondPrecision: TimeValuePrecision.microseconds,
  defaultSubSecondPrecision: TimeValuePrecision.milliseconds
};

const postgresOptions = {
  subSecondPrecision: TimeValuePrecision.microseconds,
  defaultSubSecondPrecision: TimeValuePrecision.microseconds
};

const mongoOptions = {
  subSecondPrecision: TimeValuePrecision.milliseconds,
  defaultSubSecondPrecision: TimeValuePrecision.milliseconds
};
