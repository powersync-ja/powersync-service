import { container } from '@powersync/lib-services-framework';
import { beforeAll } from 'vitest';

beforeAll(() => {
  // Executes for every test file
  container.registerDefaults();
});
