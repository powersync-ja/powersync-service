import { register } from '@powersync/service-core-tests';
import { describe } from 'vitest';
import { INITIALIZED_MONGO_STORAGE_FACTORY } from './util.js';

describe('sync - mongodb', () => register.registerSyncTests(INITIALIZED_MONGO_STORAGE_FACTORY));