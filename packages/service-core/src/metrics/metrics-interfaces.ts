export interface Counter {
  /**
   *  Increment the counter by the given value. Only positive numbers are valid.
   *  @param value
   */
  add(value: number): void;
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
  setValueProvider(valueProvider: () => Promise<number | undefined>): void;
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
