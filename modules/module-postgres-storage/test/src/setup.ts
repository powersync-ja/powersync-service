import { container } from '@powersync/lib-services-framework';
import { METRICS_HELPER } from '@powersync/service-core-tests';
import { beforeAll } from 'vitest';

beforeAll(async () => {
  // Executes for every test file
  container.registerDefaults();

  METRICS_HELPER.resetMetrics();
});
