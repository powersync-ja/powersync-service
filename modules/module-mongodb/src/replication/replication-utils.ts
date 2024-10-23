import * as mongo from 'mongodb';
import { MongoManager } from './MongoManager.js';

export async function checkSourceConfiguration(connectionManager: MongoManager): Promise<void> {
  const db = connectionManager.db;
  const hello = await db.command({ hello: 1 });
  if (hello.msg == 'isdbgrid') {
    throw new Error('Sharded MongoDB Clusters are not supported yet (including MongoDB Serverless instances).');
  } else if (hello.setName == null) {
    throw new Error('Standalone MongoDB instances are not supported - use a replicaset.');
  }
}
