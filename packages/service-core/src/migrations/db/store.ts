import * as micro_migrate from '@journeyapps-platform/micro-migrate';
import * as utils from '../../util/util-index.js';

const config = await utils.loadConfig();

export default micro_migrate.createMongoMigrationStore({
  uri: config.storage.uri,
  database: config.storage.database,
  username: config.storage.username,
  password: config.storage.password
});
