import {
  applyValueContext,
  CompatibilityContext,
  evaluateOperator,
  TimeValue,
  toSyncRulesValue
} from '../../src/index.js';
import { describe, expect, test } from 'vitest';

describe('toSyncRulesValue', () => {
  test('custom value', () => {
    expect(
      applyValueContext(
        toSyncRulesValue([1n, 'two', [new TimeValue('2025-08-19T00:00:00')]]),
        CompatibilityContext.FULL_BACKWARDS_COMPATIBILITY
      )
    ).toStrictEqual('[1,"two",["2025-08-19 00:00:00"]]');

    expect(
      applyValueContext(
        toSyncRulesValue({ foo: { bar: new TimeValue('2025-08-19T00:00:00') } }),
        CompatibilityContext.FULL_BACKWARDS_COMPATIBILITY
      )
    ).toStrictEqual('{"foo":{"bar":"2025-08-19 00:00:00"}}');
  });
});
