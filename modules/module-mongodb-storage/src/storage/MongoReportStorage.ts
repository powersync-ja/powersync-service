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
    const endDate = data.range?.end_date ? new Date(data.range.end_date) : new Date();
    const startDate = new Date(range.start_date);
    return {
      $lte: endDate,
      $gt: startDate
    };
  }

  private timeFrameQuery(timeframe: event_types.TimeFrames, interval: number = 1): mongo.Filter<mongo.Document> {
    const { year, month, today, parsedDate } = this.parseJsDate(new Date());
    switch (timeframe) {
      case 'month': {
        return { $lte: parsedDate, $gt: new Date(year, parsedDate.getMonth() - interval) };
      }
      case 'week': {
        const weekStartDate = new Date(parsedDate);
        weekStartDate.setDate(weekStartDate.getDate() - 6 * interval);
        const weekStart = this.parseJsDate(weekStartDate);
        return {
          $lte: parsedDate,
          $gt: new Date(weekStart.year, weekStart.month, weekStart.today)
        };
      }
      case 'hour': {
        // Get the last hour from the current time
        const previousHour = parsedDate.getHours() - interval;
        return {
          $gt: new Date(year, month, today, previousHour),
          $lte: new Date(year, month, today, parsedDate.getHours())
        };
      }
      default: {
        return {
          $lte: parsedDate,
          $gt: new Date(year, month, today - interval)
        };
      }
    }
  }

  private timeFrameDeleteQuery(timeframe: event_types.TimeFrames, interval: number = 1): mongo.Filter<mongo.Document> {
    const { year, month, today, parsedDate } = this.parseJsDate(new Date());
    switch (timeframe) {
      case 'month': {
        return { $lt: new Date(year, parsedDate.getMonth() - interval) };
      }
      case 'week': {
        const weekStartDate = new Date(parsedDate);
        weekStartDate.setDate(weekStartDate.getDate() - 6 * interval);
        const { month, year, today } = this.parseJsDate(weekStartDate);
        return {
          $lt: new Date(year, month, today)
        };
      }
      case 'hour': {
        const previousHour = parsedDate.getHours() - interval;
        return {
          $lt: new Date(year, month, today, previousHour)
        };
      }
      default: {
        return {
          $lt: new Date(year, month, today - interval)
        };
      }
    }
  }

  async deleteOldSdkData(data: event_types.DeleteOldSdkData): Promise<void> {
    const { interval, timeframe } = data;
    const timeframeFilter = this.timeFrameDeleteQuery(timeframe, interval);
    const result = await this.db.sdk_report_events.deleteMany({
      connect_at: timeframeFilter,
      $or: [{ disconnect_at: { $exists: true } }, { jwt_exp: { $lt: new Date() }, disconnect_at: { $exists: false } }]
    });
    if (result.deletedCount > 0) {
      logger.info(
        `TTL ${interval}/${timeframe}: ${result.deletedCount} MongoDB documents have been removed from sdk_report_events.`
      );
    }
  }

  async scrapeSdkData(data: event_types.ScrapeSdkDataRequest): Promise<event_types.ListCurrentConnections> {
    const { interval, timeframe } = data;
    const timeframeFilter = this.timeFrameQuery(timeframe, interval);
    const result = await this.db.sdk_report_events
      .aggregate<event_types.ListCurrentConnections>([
        {
          $match: {
            connect_at: timeframeFilter
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
    const updateFilter = this.updateDocFilter(data.user_id, data.client_id!);
    await this.db.sdk_report_events.findOneAndUpdate(updateFilter, {
      $set: {
        disconnect_at: data.disconnect_at
      },
      $unset: {
        jwt_exp: ''
      }
    });
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
            connect_at: timeframeFilter
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
