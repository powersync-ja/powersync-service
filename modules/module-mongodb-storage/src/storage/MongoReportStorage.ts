import { mongo } from '@powersync/lib-service-mongodb';
import { storage } from '@powersync/service-core';
import { event_types } from '@powersync/service-types';
import { PowerSyncMongo } from './implementation/db.js';
import {
  ListCurrentConnections,
  ListCurrentConnectionsResponse,
  SdkConnectBucketData,
  SdkDisconnectEventData
} from '@powersync/service-types/dist/events.js';

function dateFilter(userId: string, clientId: string): mongo.Filter<mongo.Document> {
  const date = new Date();
  const year = date.getFullYear();
  const month = date.getMonth() + 1;
  const day = date.getDate();
  return {
    user_id: userId,
    client_id: clientId,
    connect_at: {
      $gte: new Date(year, month, day, 0, 0, 0),
      $lt: new Date(year, month, day + 1, 0, 0, 0)
    }
  };
}

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

  async reportSdkConnect(data: SdkConnectBucketData): Promise<void> {
    await this.db.sdk_report_events.findOneAndUpdate(
      dateFilter(data.user_id, data.client_id!),
      {
        $set: data,
        $unset: {
          disconnect_at: ''
        }
      },
      {
        upsert: true
      }
    );
  }
  async reportSdkDisconnect(data: SdkDisconnectEventData): Promise<void> {
    await this.db.sdk_report_events.findOneAndUpdate(dateFilter(data.user_id, data.client_id!), {
      $set: {
        disconnect_at: data.disconnect_at
      },
      $unset: {
        jwt_exp: ''
      }
    });
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
