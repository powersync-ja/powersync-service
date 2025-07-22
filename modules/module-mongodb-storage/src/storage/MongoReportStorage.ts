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
function updateDocFilter(userId: string, clientId: string): mongo.Filter<mongo.Document> {
  const { year, month, today } = parseDate(new Date());
  const nextDay = today + 1;
  return {
    user_id: userId,
    client_id: clientId,
    connect_at: {
      $gte: new Date(year, month, today, 0, 0, 0),
      $lt: new Date(year, month, nextDay, 0, 0, 0)
    }
  };
}

function timeSpan(timeframe: event_types.TimeFrames): mongo.Filter<mongo.Document> {
  const date = new Date();
  const { year, month, day, today } = parseDate(date);
  switch (timeframe) {
    case event_types.TimeFrames.MONTH: {
      // Cron should run the first day of the new month, this then retrieves from the 1st to the last day of the month
      const thisMonth = month;
      const nextMonth = month == 11 ? 0 : month + 1;
      return {
        connect_at: { $gte: new Date(year, thisMonth, 1, 0, 0, 0), $lte: new Date(year, nextMonth, 1, 0, 0, 0) }
      };
    }
    case event_types.TimeFrames.WEEK: {
      // Back tracks the date to the previous week Monday to Sunday
      const daysToSunday = 0 - day;
      const weekEndDate = new Date(date);
      weekEndDate.setDate(weekEndDate.getDate() + daysToSunday);
      const weekStartDate = new Date(weekEndDate);
      weekStartDate.setDate(weekStartDate.getDate() - 6);
      const weekStart = parseDate(weekStartDate);
      const weekEnd = parseDate(weekEndDate);
      return {
        connect_at: {
          $gte: new Date(weekStart.year, weekStart.month, weekStart.today, 0, 0, 0),
          $lte: new Date(weekEnd.year, weekEnd.month, weekEnd.today, 0, 0, 0)
        }
      };
    }
    default: {
      // Start from today to just before tomorrow
      const tomorrow = today + 1;
      return {
        connect_at: {
          $gte: new Date(date.getFullYear(), date.getMonth(), today, 0, 0, 0),
          $lte: new Date(date.getFullYear(), date.getMonth(), tomorrow, 0, 0, 0)
        }
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
    console.log(data);
  }

  async scrapeSdkData(data: event_types.ScrapeSdkDataRequest): Promise<event_types.ListCurrentConnectionsResponse> {
    const result = await this.db.sdk_report_events
      .aggregate([
        {
          $match: {
            connect_at: timeSpan(data.scrape_time)
          }
        },
        {
          $facet: {
            unique_user_ids: [
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
            ]
          }
        }
      ])
      .toArray();
    console.log(result[0]);
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
  async listCurrentConnections(data: event_types.InstanceRequest): Promise<event_types.ListCurrentConnectionsResponse> {
    const result = await this.db.sdk_report_events
      .aggregate<event_types.ListCurrentConnections>([
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
