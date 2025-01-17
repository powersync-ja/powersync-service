import { register } from '@powersync/service-core-tests';
import { describe } from 'vitest';
import { POSTGRES_STORAGE_FACTORY } from './util.js';

describe('Postgres Sync Bucket Storage Compact', () => register.registerCompactTests(POSTGRES_STORAGE_FACTORY, {}));
