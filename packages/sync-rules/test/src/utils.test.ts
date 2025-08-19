import {
  applyValueContext,
  CompatibilityContext,
  evaluateOperator,
  DateTimeValue,
  toSyncRulesValue,
  TimeValue,
  CompatibilityLevel
} from '../../src/index.js';
import { describe, expect, test } from 'vitest';

describe('toSyncRulesValue', () => {
  test('custom value', () => {
    expect(
      applyValueContext(
        toSyncRulesValue([1n, 'two', [new DateTimeValue('2025-08-19T00:00:00')]]),
        CompatibilityContext.FULL_BACKWARDS_COMPATIBILITY
      )
    ).toStrictEqual('[1,"two",["2025-08-19 00:00:00"]]');

    expect(
      applyValueContext(
        toSyncRulesValue({ foo: { bar: new DateTimeValue('2025-08-19T00:00:00') } }),
        CompatibilityContext.FULL_BACKWARDS_COMPATIBILITY
      )
    ).toStrictEqual('{"foo":{"bar":"2025-08-19 00:00:00"}}');
  });

  test('time value', () => {
    expect(
      TimeValue.parse('12:13:14')?.toSqliteValue(new CompatibilityContext(CompatibilityLevel.SYNC_STREAMS))
    ).toStrictEqual('12:13:14.000000');
    expect(
      TimeValue.parse('12:13:14')?.toSqliteValue(new CompatibilityContext(CompatibilityLevel.LEGACY))
    ).toStrictEqual('12:13:14');

    expect(
      TimeValue.parse('12:13:14.15')?.toSqliteValue(new CompatibilityContext(CompatibilityLevel.SYNC_STREAMS))
    ).toStrictEqual('12:13:14.150000');
    expect(
      TimeValue.parse('12:13:14.15')?.toSqliteValue(new CompatibilityContext(CompatibilityLevel.LEGACY))
    ).toStrictEqual('12:13:14.15');
  });
});
