import { container } from '@powersync/lib-services-framework';
import { METRICS_HELPER } from '@powersync/service-core-tests';
import { beforeAll, beforeEach } from 'vitest';

beforeAll(async () => {
  container.registerDefaults();
});

beforeEach(async () => {
  METRICS_HELPER.resetMetrics();
});
