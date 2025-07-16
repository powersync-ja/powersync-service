import { mongo } from '@powersync/lib-service-mongodb';
import { storage } from '@powersync/service-core';
import { event_types } from '@powersync/service-types';
import { PowerSyncMongo } from './implementation/db.js';
import { SdkConnectDocument } from './implementation/models.js';

// import { SdkConnectDocument } from './implementation/models.js';
export class MongoReportStorage implements storage.ReportStorageFactory {
  private readonly client: mongo.MongoClient;
  public readonly db: PowerSyncMongo;

  constructor(db: PowerSyncMongo) {
    this.client = db.client;
    this.db = db;
  }

  async scrapeSdkData(data: event_types.PaginatedInstanceRequest): Promise<void> {
    console.log('MongoReportStorage.scrapeSdkData', data);
  }

  async reportSdkConnect(data: SdkConnectDocument): Promise<void> {
    await this.db.sdk_report_events.insertOne(data);
  }
  async reportSdkDisconnect(data: SdkConnectDocument): Promise<void> {
    const { _id, ...rest } = data;
    await this.db.sdk_report_events.findOneAndUpdate({ _id }, { $set: rest }, { upsert: true });
  }
  async listCurrentConnections(data: event_types.PaginatedInstanceRequest): Promise<void> {
    console.log('MongoReportStorage.listCurrentConnections', data);
  }

  async [Symbol.asyncDispose]() {
    // No-op
  }
}
