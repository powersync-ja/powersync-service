/**
 *  Bounded-cardinality attributes (labels) attached to a metric data point.
 *  Only use low-cardinality enum-like values here — never user/client/request identifiers.
 */
export type MetricAttributes = Record<string, string | number | boolean>;

export interface Counter {
  /**
   *  Increment the counter by the given value. Only positive numbers are valid.
   *  @param value
   *  @param attributes optional low-cardinality labels for this increment
   */
  add(value: number, attributes?: MetricAttributes): void;
}

export interface UpDownCounter {
  /**
   *  Increment or decrement(if negative) the counter by the given value.
   *  @param value
   */
  add(value: number): void;
}

export interface ObservableGauge {
  /**
   *  Set a value provider that provides the value for the gauge at the time of observation.
   *  @param valueProvider
   */
  setValueProvider(valueProvider: () => Promise<number | ObservableGaugeObservation[] | undefined>): void;
}

export interface ObservableGaugeObservation {
  value: number;
  attributes?: Record<string, string>;
}

export enum Precision {
  INT = 'int',
  DOUBLE = 'double'
}

export interface MetricMetadata {
  name: string;
  description?: string;
  unit?: string;
  precision?: Precision;
}

export interface MetricsFactory {
  createCounter(metadata: MetricMetadata): Counter;
  createUpDownCounter(metadata: MetricMetadata): UpDownCounter;
  createObservableGauge(metadata: MetricMetadata): ObservableGauge;
}
