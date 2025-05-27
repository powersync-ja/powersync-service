import { Meter, ValueType } from '@opentelemetry/api';
import {
  Counter,
  ObservableGauge,
  UpDownCounter,
  MetricMetadata,
  MetricsFactory,
  Precision,
  Gauge
} from '../metrics-interfaces.js';

export class OpenTelemetryMetricsFactory implements MetricsFactory {
  private meter: Meter;

  constructor(meter: Meter) {
    this.meter = meter;
  }

  createCounter(metadata: MetricMetadata): Counter {
    return this.meter.createCounter(metadata.name, {
      description: metadata.description,
      unit: metadata.unit,
      valueType: this.toValueType(metadata.precision)
    });
  }

  createGauge(metadata: MetricMetadata): Gauge {
    return this.meter.createGauge(metadata.name, {
      description: metadata.description,
      unit: metadata.unit,
      valueType: this.toValueType(metadata.precision)
    });
  }

  createObservableGauge(metadata: MetricMetadata): ObservableGauge {
    const gauge = this.meter.createObservableGauge(metadata.name, {
      description: metadata.description,
      unit: metadata.unit,
      valueType: this.toValueType(metadata.precision)
    });

    return {
      setValueProvider(valueProvider: () => Promise<number | undefined>) {
        gauge.addCallback(async (result) => {
          const value = await valueProvider();

          if (value != undefined) {
            result.observe(value);
          }
        });
      }
    };
  }

  createUpDownCounter(metadata: MetricMetadata): UpDownCounter {
    return this.meter.createUpDownCounter(metadata.name, {
      description: metadata.description,
      unit: metadata.unit,
      valueType: this.toValueType(metadata.precision)
    });
  }

  private toValueType(precision?: Precision): ValueType {
    if (!precision) {
      return ValueType.INT;
    }

    switch (precision) {
      case Precision.INT:
        return ValueType.INT;
      case Precision.DOUBLE:
        return ValueType.DOUBLE;
    }
  }
}
