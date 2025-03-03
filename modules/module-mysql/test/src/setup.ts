import { container } from '@powersync/lib-services-framework';
import { METRICS_HELPER, test_utils } from '@powersync/service-core-tests';
import { beforeAll, beforeEach } from 'vitest';

beforeAll(async () => {
  // Executes for every test file
  container.registerDefaults();
});

beforeEach(async () => {
  METRICS_HELPER.resetMetrics();
});
