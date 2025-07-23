import { mongo } from '@powersync/lib-service-mongodb';
import { storage } from '@powersync/service-core';
import { event_types } from '@powersync/service-types';
import { PowerSyncMongo } from './implementation/db.js';
import { logger } from '@powersync/lib-services-framework';

function parseDate(date: Date) {
  const year = date.getFullYear();
  const month = date.getMonth();
  const today = date.getDate();
  const day = date.getDay();
  return {
    year,
    month,
    today,
    day
  };
}
function updateDocFilter(userId: string, clientId: string) {
  const { year, month, today } = parseDate(new Date());
  const nextDay = today + 1;
  return {
    user_id: userId,
    client_id: clientId,
    connect_at: {
      $gte: new Date(year, month, today),
      $lt: new Date(year, month, nextDay)
    }
  };
}

function timeSpan(period: event_types.TimeFrames, timeframe: number = 1): mongo.Filter<mongo.Document> {
  const date = new Date();
  const { year, month, today } = parseDate(date);
  switch (period) {
    case 'month': {
      return { $lte: date, $gte: new Date(year, date.getMonth() - timeframe) };
    }
    case 'week': {
      const weekStartDate = new Date(date);
      weekStartDate.setDate(weekStartDate.getDate() - 6 * timeframe);
      const weekStart = parseDate(weekStartDate);
      return {
        $gte: new Date(weekStart.year, weekStart.month, weekStart.today),
        $lte: date
      };
    }
    case 'hour': {
      // Get the last hour from the current time
      const previousHour = date.getHours() - timeframe;
      return {
        $gte: new Date(year, month, today, previousHour),
        $lt: new Date(year, month, today, date.getHours())
      };
    }
    default: {
      // Start from today to just before tomorrow
      return {
        $lte: date,
        $gte: new Date(year, month, today - timeframe)
      };
    }
  }
}

export class MongoReportStorage implements storage.ReportStorageFactory {
  private readonly client: mongo.MongoClient;
  public readonly db: PowerSyncMongo;

  constructor(db: PowerSyncMongo) {
    this.client = db.client;
    this.db = db;
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

  async deleteOldSdkData(data: event_types.DeleteOldSdkData): Promise<void> {
    const { period, timeframe } = data;
    const result = await this.db.sdk_report_events.deleteMany({
      connect_at: timeSpan(period, timeframe),
      $or: [{ disconnect_at: { $exists: true } }, { jwt_exp: { $lt: new Date() }, disconnect_at: { $exists: false } }]
    });
    console.log(result);
    if (result.deletedCount > 0) {
      logger.info(`TTL: ${result.deletedCount} documents have been removed from sdk_report_events collection`);
    }
  }

  async scrapeSdkData(data: event_types.ScrapeSdkDataRequest): Promise<event_types.ListCurrentConnections> {
    const timespanFilter = timeSpan(data.period, data.interval);
    console.log({ timespanFilter });
    const result = await this.db.sdk_report_events
      .aggregate([
        {
          $match: {
            connect_at: timespanFilter
          }
        },
        this.sdkFacetPipeline(),
        this.sdkProjectPipeline()
      ])
      .toArray();
    return result[0] as event_types.ListCurrentConnections;
  }

  async reportSdkConnect(data: event_types.SdkConnectBucketData): Promise<void> {
    await this.db.sdk_report_events.findOneAndUpdate(
      updateDocFilter(data.user_id, data.client_id!),
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
    await this.db.sdk_report_events.findOneAndUpdate(updateDocFilter(data.user_id, data.client_id!), {
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
    const timespanFilter = data.period ? { connect_at: timeSpan(data.period) } : undefined;
    const result = await this.db.sdk_report_events
      .aggregate([
        {
          $match: {
            disconnect_at: { $exists: false },
            jwt_exp: { $gt: new Date() },
            ...timespanFilter
          }
        },
        this.sdkFacetPipeline(),
        this.sdkProjectPipeline()
      ])
      .toArray();
    return result[0] as event_types.ListCurrentConnections;
  }

  async [Symbol.asyncDispose]() {
    // No-op
  }
}
