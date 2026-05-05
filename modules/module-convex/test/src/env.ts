import { utils } from '@powersync/lib-services-framework';
import fs from 'fs';
import path from 'node:path';
function obtainDefaultLocalConvexDeployKey(): string | null {
  const localConfigPath = path.join(import.meta.dirname, '../../.convex/local/default/config.json');
  try {
    if (!fs.existsSync(localConfigPath)) {
      return null;
    }

    const content = JSON.parse(fs.readFileSync(localConfigPath, 'utf8'));
    return content.adminKey;
  } catch (ex) {
    console.warn(`Could not find local convex config in .convex`);
    return null;
  }
}

export const env = utils.collectEnvironmentVariables({
  CONVEX_URL: utils.type.string.default('http://127.0.0.1:3210'),
  CONVEX_DEPLOY_KEY: utils.type.string.default(obtainDefaultLocalConvexDeployKey() ?? ''),
  CONVEX_DEPLOYMENT: utils.type.string.default('anonymous:anonymous-module-convex'),
  MONGO_TEST_URL: utils.type.string.default('mongodb://127.0.0.1:27017/powersync_test?directConnection=true'),
  PG_STORAGE_TEST_URL: utils.type.string.default('postgres://postgres:postgres@localhost:5432/powersync_storage_test'),
  CI: utils.type.boolean.default('false'),
  SLOW_TESTS: utils.type.boolean.default('false'),
  TEST_MONGO_STORAGE: utils.type.boolean.default('true'),
  TEST_POSTGRES_STORAGE: utils.type.boolean.default('true')
});
