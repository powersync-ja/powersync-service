import { register } from '@powersync/service-core-tests';
import { describe } from 'vitest';
import { POSTGRES_STORAGE_FACTORY } from './util.js';

/**
 * Bucket compacting is not yet implemented.
 * This causes the internal compacting test to fail.
 * Other tests have been verified manually.
 */
describe('sync - postgres', () => {
  register.registerSyncTests(POSTGRES_STORAGE_FACTORY);
});
