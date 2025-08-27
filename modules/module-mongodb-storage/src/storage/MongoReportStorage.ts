import { mongo } from '@powersync/lib-service-mongodb';
import { storage } from '@powersync/service-core';
import { event_types } from '@powersync/service-types';
import { PowerSyncMongo } from './implementation/db.js';
import { logger } from '@powersync/lib-services-framework';

export class MongoReportStorage implements storage.ReportStorage {
  private readonly client: mongo.MongoClient;
  public readonly db: PowerSyncMongo;

  constructor(db: PowerSyncMongo) {
    this.client = db.client;
    this.db = db;
  }
  async deleteOldConnectionData(data: event_types.DeleteOldConnectionData): Promise<void> {
    const { date } = data;
    const result = await this.db.connection_report_events.deleteMany({
      connected_at: { $lt: date },
      $or: [
        { disconnected_at: { $exists: true } },
        { jwt_exp: { $lt: new Date() }, disconnected_at: { $exists: false } }
      ]
    });
    if (result.deletedCount > 0) {
      logger.info(
        `TTL from ${date.toISOString()}: ${result.deletedCount} MongoDB documents have been removed from connection_report_events.`
      );
    }
  }

  async getClientConnectionReports(
    data: event_types.ClientConnectionReportRequest
  ): Promise<event_types.ClientConnectionReportResponse> {
    const { start, end } = data;
    const result = await this.db.connection_report_events
      .aggregate<event_types.ClientConnectionReportResponse>([
        {
          $match: {
            connected_at: { $lte: end, $gte: start }
          }
        },
        this.connectionsFacetPipeline(),
        this.connectionsProjectPipeline()
      ])
      .toArray();
    return result[0];
  }

  async reportClientConnection(data: event_types.ClientConnectionBucketData): Promise<void> {
    const updateFilter = this.updateDocFilter(data.user_id, data.client_id!);
    await this.db.connection_report_events.findOneAndUpdate(
      updateFilter,
      {
        $set: data,
        $unset: {
          disconnected_at: ''
        }
      },
      {
        upsert: true
      }
    );
  }
  async reportClientDisconnection(data: event_types.ClientDisconnectionEventData): Promise<void> {
    const { connected_at, user_id, client_id } = data;
    await this.db.connection_report_events.findOneAndUpdate(
      {
        client_id,
        user_id,
        connected_at
      },
      {
        $set: {
          disconnected_at: data.disconnected_at
        },
        $unset: {
          jwt_exp: ''
        }
      }
    );
  }
  async getConnectedClients(): Promise<event_types.ClientConnectionReportResponse> {
    const result = await this.db.connection_report_events
      .aggregate<event_types.ClientConnectionReportResponse>([
        {
          $match: {
            disconnected_at: { $exists: false },
            jwt_exp: { $gt: new Date() }
          }
        },
        this.connectionsFacetPipeline(),
        this.connectionsProjectPipeline()
      ])
      .toArray();
    return result[0];
  }

  async [Symbol.asyncDispose]() {
    // No-op
  }

  private parseJsDate(date: Date) {
    const year = date.getUTCFullYear();
    const month = date.getUTCMonth();
    const today = date.getUTCDate();
    const day = date.getUTCDay();
    return {
      year,
      month,
      today,
      day,
      parsedDate: date
    };
  }

  private connectionsFacetPipeline() {
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

  private connectionsProjectPipeline() {
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
      connected_at: {
        $gte: new Date(Date.UTC(year, month, today)),
        $lt: new Date(Date.UTC(year, month, nextDay))
      }
    };
  }
}
