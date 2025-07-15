import { mongo } from '@powersync/lib-service-mongodb';
import { storage } from '@powersync/service-core';
import { event_types } from '@powersync/service-types';
import { PowerSyncMongo } from './implementation/db.js';

export class MongoReportStorage implements storage.ReportStorageFactory {
  private readonly client: mongo.MongoClient;
  public readonly db: PowerSyncMongo;

  constructor(db: PowerSyncMongo) {
    this.client = db.client;
    this.db = db;
  }

  async reportSdkConnect(data: event_types.SdkConnectEventData): Promise<void> {
    console.log('MongoReportStorage.reportSdkConnect', data);
  }
  async reportSdkDisconnect(data: event_types.SdkDisconnectEventData): Promise<void> {
    console.log('MongoReportStorage.reportSdkDisconnect', data);
  }
  async listCurrentConnections(data: event_types.CurrentConnectionsData): Promise<void> {
    console.log('MongoReportStorage.listCurrentConnections', data);
  }

  async [Symbol.asyncDispose]() {
    // No-op
  }
}
