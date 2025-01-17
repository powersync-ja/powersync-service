import { register } from '@powersync/service-core-tests';
import { describe } from 'vitest';
import { MongoMigrationAgent } from '../../src/migrations/MongoMigrationAgent.js';
import { env } from './env.js';

const MIGRATION_AGENT_FACTORY = () => {
  return new MongoMigrationAgent({ type: 'mongodb', uri: env.MONGO_TEST_URL });
};

describe('Mongo Migrations Store', () => register.registerMigrationTests(MIGRATION_AGENT_FACTORY));
