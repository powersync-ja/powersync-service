import { container } from '@powersync/lib-services-framework';
import { Metrics } from '@powersync/service-core';
import { beforeAll } from 'vitest';

beforeAll(async () => {
  // Executes for every test file
  container.registerDefaults();

  // The metrics need to be initialized before they can be used
  await Metrics.initialise({
    disable_telemetry_sharing: true,
    powersync_instance_id: 'test',
    internal_metrics_endpoint: 'unused.for.tests.com'
  });
  Metrics.getInstance().resetCounters();
});
