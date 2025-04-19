import { container } from '@powersync/lib-services-framework';
import { beforeAll, beforeEach } from 'vitest';
import { METRICS_HELPER } from '@powersync/service-core-tests';

beforeAll(async () => {
  // Executes for every test file
  container.registerDefaults();
});

beforeEach(async () => {
  METRICS_HELPER.resetMetrics();
});
