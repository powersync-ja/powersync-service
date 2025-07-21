import { mongo } from '@powersync/lib-service-mongodb';
import { storage } from '@powersync/service-core';
import { event_types } from '@powersync/service-types';
import { PowerSyncMongo } from './implementation/db.js';
import { SdkConnectDocument } from './implementation/models.js';
import { ListCurrentConnectionsResponse } from '@powersync/service-types/dist/events.js';

export class MongoReportStorage implements storage.ReportStorageFactory {
  private readonly client: mongo.MongoClient;
  public readonly db: PowerSyncMongo;

  constructor(db: PowerSyncMongo) {
    this.client = db.client;
    this.db = db;
  }

  async scrapeSdkData(data: event_types.InstanceRequest): Promise<void> {
    console.log('MongoReportStorage.scrapeSdkData', data);
  }

  async reportSdkConnect(data: SdkConnectDocument): Promise<void> {
    await this.db.sdk_report_events.findOneAndUpdate(
      { user_id: data.user_id, client_id: data.client_id },
      {
        $set: data
      },
      {
        upsert: true
      }
    );
  }
  async reportSdkDisconnect(data: SdkConnectDocument): Promise<void> {
    await this.db.sdk_report_events.findOneAndDelete({ user_id: data.user_id, client_id: data.client_id });
  }
  async listCurrentConnections(data: event_types.InstanceRequest): Promise<ListCurrentConnectionsResponse> {
    const result = await this.db.sdk_report_events
      .aggregate([
        {
          $group: {
            _id: null,
            user_ids: { $addToSet: '$user_id' },
            client_ids: { $addToSet: '$client_id' },
            sdks: { $addToSet: '$sdk' }
          }
        },
        {
          $project: {
            _id: 0,
            user_count: { $size: '$user_ids' },
            client_id_count: { $size: '$client_ids' },
            sdk: '$sdks'
          }
        }
      ])
      .toArray();
    console.log(result);
    return {
      ...data,
      ...result[0]
    };
  }

  async [Symbol.asyncDispose]() {
    // No-op
  }
}
