import { env } from './env.js';

import { MongoTestStorageFactoryGenerator } from '@module/storage/implementation/MongoTestStorageFactoryGenerator.js';

export const INITIALIZED_MONGO_STORAGE_FACTORY = MongoTestStorageFactoryGenerator({
  url: env.MONGO_TEST_URL,
  isCI: env.CI
});
