import { container } from '@powersync/lib-services-framework';
import { METRICS_HELPER } from '@powersync/service-core-tests';
import { afterEach, beforeAll, beforeEach } from 'vitest';
import { cleanupS3TestStorage } from './helpers/s3TestFactory.js';

beforeAll(async () => {
  // Executes for every test file
  container.registerDefaults();
});

beforeEach(async () => {
  METRICS_HELPER.resetMetrics();
});

afterEach(async () => {
  await cleanupS3TestStorage();
});
