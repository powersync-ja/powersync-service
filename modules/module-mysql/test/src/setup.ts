import { container } from '@powersync/lib-services-framework';
import { test_utils } from '@powersync/service-core-tests';
import { beforeAll, beforeEach } from 'vitest';

beforeAll(async () => {
  // Executes for every test file
  container.registerDefaults();
  await test_utils.initMetrics();
});

beforeEach(async () => {
  await test_utils.resetMetrics();
});
