import { describe, expect, test } from 'vitest';
import {
  applyValueContext,
  BucketDataSource,
  CompatibilityContext,
  CompatibilityEdition,
  CompatibilityOption,
  DateTimeSourceOptions,
  DateTimeValue,
  mergeBuckets,
  ResolvedBucket,
  TimeValue,
  TimeValuePrecision,
  toSyncRulesValue
} from '../../src/index.js';

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

  test('booleans in json', () => {
    expect(
      applyValueContext(toSyncRulesValue([1n, true]), CompatibilityContext.FULL_BACKWARDS_COMPATIBILITY)
    ).toStrictEqual('[1,1]');

    expect(
      applyValueContext(
        toSyncRulesValue([1n, true]),
        new CompatibilityContext({
          edition: CompatibilityEdition.COMPILED_STREAMS,
          overrides: new Map([[CompatibilityOption.fixedBooleanInJson, true]])
        })
      )
    ).toStrictEqual('[1,true]');
  });
});

describe('mergeBuckets', () => {
  test('merges single bucket', () => {
    const fakeSource: BucketDataSource = null as any;
    const a: ResolvedBucket = {
      definition: 'a',
      inclusion_reasons: ['default'],
      priority: 3,
      bucket: 'bkt',
      source: fakeSource
    };
    const b: ResolvedBucket = {
      definition: 'a',
      inclusion_reasons: [{ subscription: 1 }],
      priority: 2,
      bucket: 'bkt',
      source: fakeSource
    };

    expect(mergeBuckets([a, b])).toStrictEqual([
      {
        definition: 'a',
        inclusion_reasons: ['default', { subscription: 1 }],
        priority: 2,
        bucket: 'bkt'
      }
    ]);
  });

  test('deduplicates matching inclusion reasons', () => {
    const fakeSource: BucketDataSource = null as any;
    const a: ResolvedBucket = {
      definition: 'a',
      inclusion_reasons: ['default', { subscription: 1 }],
      priority: 3,
      bucket: 'bkt',
      source: fakeSource
    };
    const b: ResolvedBucket = {
      definition: 'a',
      inclusion_reasons: [{ subscription: 1 }, { subscription: 2 }],
      priority: 2,
      bucket: 'bkt',
      source: fakeSource
    };

    expect(mergeBuckets([a, b])).toStrictEqual([
      {
        definition: 'a',
        inclusion_reasons: ['default', { subscription: 1 }, { subscription: 2 }],
        priority: 2,
        bucket: 'bkt'
      }
    ]);
  });
});
