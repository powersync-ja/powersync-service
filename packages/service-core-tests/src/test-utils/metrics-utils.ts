import { Metrics } from '@powersync/service-core';

export const initMetrics = async () => {
  await Metrics.initialise({
    disable_telemetry_sharing: true,
    powersync_instance_id: 'test',
    internal_metrics_endpoint: 'unused.for.tests.com'
  });
  Metrics.getInstance().resetCounters();
};
