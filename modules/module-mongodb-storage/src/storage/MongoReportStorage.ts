import { mongo } from '@powersync/lib-service-mongodb';
import { storage } from '@powersync/service-core';
import { event_types } from '@powersync/service-types';
import { PowerSyncMongo } from './implementation/db.js';

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

function parseMonthInterval(currentMonth: number, interval: number) {
  const readableMonth = currentMonth + 1;
  const difference = readableMonth - interval;
  if (difference < 0) {
    return 12 + difference - 1;
  }
  return difference - 1;
}

function timeSpan(period: event_types.TimeFrames, timeframe: number = 1): mongo.Filter<mongo.Document> {
  const date = new Date();
  const { year, month, day, today } = parseDate(date);
  switch (period) {
    case 'month': {
      const thisMonth = month;
      const previousMonth = parseMonthInterval(thisMonth, timeframe);
      return { $gte: new Date(year, thisMonth), $lt: new Date(year, previousMonth) };
    }
    case 'week': {
      // Back tracks the date to the previous week Monday to Sunday
      const daysToSunday = 0 - day;
      const weekEndDate = new Date(date);
      weekEndDate.setDate(weekEndDate.getDate() + daysToSunday);
      const weekStartDate = new Date(weekEndDate);
      weekStartDate.setDate(weekStartDate.getDate() - 6 * timeframe);
      const weekStart = parseDate(weekStartDate);
      const weekEnd = parseDate(weekEndDate);
      return {
        $gte: new Date(weekStart.year, weekStart.month, weekStart.today),
        $lte: new Date(weekEnd.year, weekEnd.month, weekEnd.today)
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
        $gte: new Date(year, month, today),
        $lte: new Date(year, month, today - timeframe)
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

  async deleteOldSdkData(data: event_types.DeleteOldSdkData): Promise<void> {
    const { period, timeframe } = data;
    await this.db.sdk_report_events.deleteMany({
      connect_at: timeSpan(period, timeframe),
      $or: [{ disconnect_at: { $exists: true } }, { jwt_exp: { $lt: new Date() }, disconnect_at: { $exists: false } }]
    });
  }

  async scrapeSdkData(data: event_types.ScrapeSdkDataRequest): Promise<event_types.ListCurrentConnectionsResponse> {
    const timespanFilter = timeSpan(data.period);
    const result = await this.db.sdk_report_events
      .aggregate([
        {
          $match: {
            connect_at: timespanFilter
          }
        },
        {
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
            unique_user_sdk: [
              {
                $group: {
                  _id: {
                    user_id: '$user_id',
                    sdk: '$sdk'
                  }
                }
              },
              {
                $count: 'count'
              }
            ],
            unique_user_client: [
              {
                $group: {
                  _id: {
                    user_id: '$user_id',
                    client_id: '$client_id'
                  }
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
                  count: { $sum: 1 }
                }
              },
              {
                $project: {
                  _id: 0,
                  k: '$_id',
                  v: '$count'
                }
              }
            ]
          }
        },
        {
          $project: {
            users: { $ifNull: [{ $arrayElemAt: ['$unique_users.count', 0] }, 0] },
            user_sdk: { $ifNull: [{ $arrayElemAt: ['$unique_user_sdk.count', 0] }, 0] },
            client_user: { $ifNull: [{ $arrayElemAt: ['$unique_user_client.count', 0] }, 0] },
            sdk_versions: { $arrayToObject: '$sdk_versions_array' }
          }
        }
      ])
      .toArray();
    return result[0] as event_types.ListCurrentConnectionsResponse;
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
  ): Promise<event_types.ListCurrentConnectionsResponse> {
    const timespanFilter = data.period ? { connect_at: timeSpan(data.period) } : undefined;
    console.log({ timespanFilter });
    const result = await this.db.sdk_report_events
      .aggregate([
        {
          $match: {
            disconnect_at: { $exists: false },
            jwt_exp: { $gt: new Date() },
            ...timespanFilter
          }
        },
        {
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
            unique_user_sdk: [
              {
                $group: {
                  _id: {
                    user_id: '$user_id',
                    sdk: '$sdk'
                  }
                }
              },
              {
                $count: 'count'
              }
            ],
            unique_user_client: [
              {
                $group: {
                  _id: {
                    user_id: '$user_id',
                    client_id: '$client_id'
                  }
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
                  count: { $sum: 1 }
                }
              },
              {
                $project: {
                  _id: 0,
                  k: '$_id',
                  v: '$count'
                }
              }
            ]
          }
        },
        {
          $project: {
            users: { $ifNull: [{ $arrayElemAt: ['$unique_users.count', 0] }, 0] },
            user_sdk: { $ifNull: [{ $arrayElemAt: ['$unique_user_sdk.count', 0] }, 0] },
            client_user: { $ifNull: [{ $arrayElemAt: ['$unique_user_client.count', 0] }, 0] },
            sdk_versions: { $arrayToObject: '$sdk_versions_array' }
          }
        }
      ])
      .toArray();
    return result[0] as event_types.ListCurrentConnectionsResponse;
  }

  async [Symbol.asyncDispose]() {
    // No-op
  }
}
