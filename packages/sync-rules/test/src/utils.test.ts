import {
  applyValueContext,
  CompatibilityContext,
  DateTimeValue,
  toSyncRulesValue,
  TimeValue,
  CompatibilityEdition,
  DateTimeSourceOptions,
  TimeValuePrecision
} from '../../src/index.js';
import { describe, expect, test } from 'vitest';

describe('toSyncRulesValue', () => {
  const legacy = new CompatibilityContext({ edition: 1 });
  const syncStreams = new CompatibilityContext({ edition: 2 });
  const sourceOptions: DateTimeSourceOptions = {
    subSecondPrecision: TimeValuePrecision.milliseconds,
    defaultSubSecondPrecision: TimeValuePrecision.milliseconds
  };

  test('custom value', () => {
    expect(
      applyValueContext(
        toSyncRulesValue([1n, 'two', [new DateTimeValue('2025-08-19T00:00:00', undefined, sourceOptions)]]),
        CompatibilityContext.FULL_BACKWARDS_COMPATIBILITY
      )
    ).toStrictEqual('[1,"two",["2025-08-19 00:00:00"]]');

    expect(
      applyValueContext(
        toSyncRulesValue({ foo: { bar: new DateTimeValue('2025-08-19T00:00:00', undefined, sourceOptions) } }),
        CompatibilityContext.FULL_BACKWARDS_COMPATIBILITY
      )
    ).toStrictEqual('{"foo":{"bar":"2025-08-19 00:00:00"}}');
  });

  test('time value', () => {
    expect(TimeValue.parse('12:13:14', sourceOptions)?.toSqliteValue(syncStreams)).toStrictEqual('12:13:14.000');
    expect(TimeValue.parse('12:13:14', sourceOptions)?.toSqliteValue(legacy)).toStrictEqual('12:13:14');

    expect(TimeValue.parse('12:13:14.15', sourceOptions)?.toSqliteValue(syncStreams)).toStrictEqual('12:13:14.150');
    expect(TimeValue.parse('12:13:14.15', sourceOptions)?.toSqliteValue(legacy)).toStrictEqual('12:13:14.15');
  });
});
