export interface MetricsRecorder {
  addBytesRead(bytes: number): void;
}

let metricsRecorder: MetricsRecorder | undefined;

/**
 * Configure a metrics recorder to capture global metrics.
 */
export function setMetricsRecorder(recorder: MetricsRecorder) {
  metricsRecorder = recorder;
}

export function recordBytesRead(bytes: number) {
  metricsRecorder?.addBytesRead(bytes);
}
