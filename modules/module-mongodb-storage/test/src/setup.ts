import { container } from '@powersync/lib-services-framework';
import { METRICS_HELPER } from '@powersync/service-core-tests';
import { afterEach, beforeAll, beforeEach } from 'vitest';
import { cleanupS3TestStorage, initializeAzureTestStorage } from './helpers/s3TestFactory.js';

beforeAll(async () => {
  // Executes for every test file
  container.registerDefaults();
  await initializeAzureTestStorage();
});

beforeEach(async () => {
  METRICS_HELPER.resetMetrics();
});

afterEach(async () => {
  await cleanupS3TestStorage();
});
