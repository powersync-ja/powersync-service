import { container } from '@powersync/lib-services-framework';
import { beforeAll } from 'vitest';

beforeAll(() => {
  // Your setup code here
  container.registerDefaults();
});
