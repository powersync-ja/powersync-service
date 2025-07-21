import { mongo } from '@powersync/lib-service-mongodb';
import { storage } from '@powersync/service-core';
import { event_types } from '@powersync/service-types';
import { PowerSyncMongo } from './implementation/db.js';
import { SdkConnectDocument } from './implementation/models.js';
import {
  ListCurrentConnections,
  ListCurrentConnectionsResponse,
  SdkDisconnectEventData
} from '@powersync/service-types/dist/events.js';

export class MongoReportStorage implements storage.ReportStorageFactory {
  private readonly client: mongo.MongoClient;
  public readonly db: PowerSyncMongo;

  constructor(db: PowerSyncMongo) {
    this.client = db.client;
    this.db = db;
  }

  async deleteOldSdkData(data: event_types.DeleteOldSdkData): Promise<void> {
    console.log(data);
  }

  async scrapeSdkData(data: event_types.InstanceRequest): Promise<ListCurrentConnectionsResponse> {
    const result = await this.db.sdk_report_events
      .aggregate<ListCurrentConnections>([
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
            users: '$user_ids',
            clients: '$client_ids',
            sdks: '$sdks'
          }
        }
      ])
      .toArray();
    return {
      app_id: data.app_id,
      org_id: data.org_id,
      ...result[0]
    };
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
  async reportSdkDisconnect(data: SdkDisconnectEventData): Promise<void> {
    await this.db.sdk_report_events.findOneAndUpdate(
      { user_id: data.user_id, client_id: data.client_id },
      {
        $set: {
          disconnect_at: data.disconnect_at
        },
        $unset: {
          jwt_exp: ''
        }
      }
    );
  }
  async listCurrentConnections(data: event_types.InstanceRequest): Promise<ListCurrentConnectionsResponse> {
    const result = await this.db.sdk_report_events
      .aggregate<ListCurrentConnections>([
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
            users: '$user_ids',
            clients: '$client_ids',
            sdks: '$sdks'
          }
        }
      ])
      .toArray();
    return {
      app_id: data.app_id,
      org_id: data.org_id,
      ...result[0]
    };
  }

  async [Symbol.asyncDispose]() {
    // No-op
  }
}
