import { mongo } from '@powersync/lib-service-mongodb';
import { storage } from '@powersync/service-core';
import { event_types } from '@powersync/service-types';
import { PowerSyncMongo } from './implementation/db.js';
import { logger } from '@powersync/lib-services-framework';

export class MongoReportStorage implements storage.ReportStorageFactory {
  private readonly client: mongo.MongoClient;
  public readonly db: PowerSyncMongo;

  constructor(db: PowerSyncMongo) {
    this.client = db.client;
    this.db = db;
  }

  private parseJsDate(date: Date) {
    const year = date.getFullYear();
    const month = date.getMonth();
    const today = date.getDate();
    const day = date.getDay();
    return {
      year,
      month,
      today,
      day,
      parsedDate: date
    };
  }

  private sdkFacetPipeline() {
    return {
      $facet: {
        unique_users: [
          {
            $group: {
              _id: '$user_id'
            }
          },
          {
            $count: 'count'
          }
        ],
        sdk_versions_array: [
          {
            $group: {
              _id: '$sdk',
              total: { $sum: 1 },
              client_ids: { $addToSet: '$client_id' },
              user_ids: { $addToSet: '$user_id' }
            }
          },
          {
            $project: {
              _id: 0,
              sdk: '$_id',
              users: { $size: '$user_ids' },
              clients: { $size: '$client_ids' }
            }
          },
          {
            $sort: {
              sdk: 1
            }
          }
        ]
      }
    };
  }

  private sdkProjectPipeline() {
    return {
      $project: {
        users: { $ifNull: [{ $arrayElemAt: ['$unique_users.count', 0] }, 0] },
        sdks: '$sdk_versions_array'
      }
    };
  }

  private updateDocFilter(userId: string, clientId: string) {
    const { year, month, today } = this.parseJsDate(new Date());
    const nextDay = today + 1;
    return {
      user_id: userId,
      client_id: clientId,
      connect_at: {
        // Need to create a new date here to sett the time to 00:00:00
        $gte: new Date(year, month, today),
        $lt: new Date(year, month, nextDay)
      }
    };
  }

  private listConnectionsDateRange(data: event_types.ListCurrentConnectionsRequest) {
    const { range } = data;
    if (!range) {
      return undefined;
    }
    const endDate = data.range?.end ? new Date(data.range.end) : new Date();
    const startDate = new Date(range.start);
    return {
      connect_at: {
        $lte: endDate,
        $gte: startDate
      }
    };
  }

  async deleteOldSdkData(data: event_types.DeleteOldSdkData): Promise<void> {
    const { date } = data;
    const result = await this.db.sdk_report_events.deleteMany({
      connect_at: { $lt: date },
      $or: [{ disconnect_at: { $exists: true } }, { jwt_exp: { $lt: new Date() }, disconnect_at: { $exists: false } }]
    });
    if (result.deletedCount > 0) {
      logger.info(
        `TTL from ${date.toISOString()}: ${result.deletedCount} MongoDB documents have been removed from sdk_report_events.`
      );
    }
  }

  async scrapeSdkData(data: event_types.ScrapeSdkDataRequest): Promise<event_types.ListCurrentConnections> {
    const { start, end } = data;
    const result = await this.db.sdk_report_events
      .aggregate<event_types.ListCurrentConnections>([
        {
          $match: {
            connect_at: { $lte: end, $gte: start }
          }
        },
        this.sdkFacetPipeline(),
        this.sdkProjectPipeline()
      ])
      .toArray();
    return result[0];
  }

  async reportSdkConnect(data: event_types.SdkConnectBucketData): Promise<void> {
    const updateFilter = this.updateDocFilter(data.user_id, data.client_id!);
    await this.db.sdk_report_events.findOneAndUpdate(
      updateFilter,
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
  async reportSdkDisconnect(data: event_types.SdkDisconnectEventData): Promise<void> {
    const { connect_at, user_id, client_id } = data;
    await this.db.sdk_report_events.findOneAndUpdate(
      {
        client_id,
        user_id,
        connect_at
      },
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
  async listCurrentConnections(
    data: event_types.ListCurrentConnectionsRequest
  ): Promise<event_types.ListCurrentConnections> {
    const timeframeFilter = this.listConnectionsDateRange(data);
    const result = await this.db.sdk_report_events
      .aggregate<event_types.ListCurrentConnections>([
        {
          $match: {
            disconnect_at: { $exists: false },
            jwt_exp: { $gt: new Date() },
            ...timeframeFilter
          }
        },
        this.sdkFacetPipeline(),
        this.sdkProjectPipeline()
      ])
      .toArray();
    return result[0];
  }

  async [Symbol.asyncDispose]() {
    // No-op
  }
}
