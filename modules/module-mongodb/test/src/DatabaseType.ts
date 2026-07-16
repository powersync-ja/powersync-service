import { detectDocumentDb } from '@module/replication/replication-utils.js';
import { logger } from '@powersync/lib-services-framework';
import { connectMongoData } from './util.js';

export enum DatabaseType {
  DOCUMENTDB = 'DOCUMENTDB',
  MONGODB = 'MONGODB'
}

let _databaseType: DatabaseType = DatabaseType.MONGODB;

// Detected once at import time. Close the client afterwards so this detection
// does not leak a connection: this module is imported by many test files, and
// the client created here is otherwise never closed, accumulating open handles
// in Vitest.
const { client, db } = await connectMongoData();
try {
  _databaseType = (await detectDocumentDb(db)) ? DatabaseType.DOCUMENTDB : DatabaseType.MONGODB;
} catch (ex) {
  logger.warn(`Could not determine MongoDB database type`, ex);
} finally {
  await client.close().catch(() => {});
}

export const DATABASE_TYPE = _databaseType;
