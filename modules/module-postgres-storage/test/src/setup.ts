import { container } from '@powersync/lib-services-framework';
import { beforeAll } from 'vitest';
import { METRICS_HELPER } from '@powersync/service-core-tests';

beforeAll(async () => {
  // Executes for every test file
  container.registerDefaults();

  METRICS_HELPER.resetMetrics();
});
