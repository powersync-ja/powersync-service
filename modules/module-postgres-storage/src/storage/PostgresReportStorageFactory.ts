import { storage } from '@powersync/service-core';
import * as pg_wire from '@powersync/service-jpgwire';
import { event_types } from '@powersync/service-types';
import { v4 } from 'uuid';
import * as lib_postgres from '@powersync/lib-service-postgres';
import { NormalizedPostgresStorageConfig } from '../types/types.js';

import { STORAGE_SCHEMA_NAME } from '../utils/db.js';
import { getStorageApplicationName } from '../utils/application-name.js';
import {
  DeleteOldSdkData,
  ListCurrentConnections,
  ListCurrentConnectionsRequest,
  ScrapeSdkDataRequest,
  SdkConnectBucketData,
  SdkDisconnectEventData
} from '@powersync/service-types/src/events.js';
import { SdkReporting, SdkReportingDecoded } from '../types/models/SdkReporting.js';

export type PostgresReportStorageOptions = {
  config: NormalizedPostgresStorageConfig;
};

export class PostgresReportStorageFactory implements storage.ReportStorageFactory {
  readonly db: lib_postgres.DatabaseClient;
  constructor(protected options: PostgresReportStorageOptions) {
    this.db = new lib_postgres.DatabaseClient({
      config: options.config,
      schema: STORAGE_SCHEMA_NAME,
      applicationName: getStorageApplicationName()
    });

    this.db.registerListener({
      connectionCreated: async (connection) => this.prepareStatements(connection)
    });
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
  private timeFrameQuery(timeframe: event_types.TimeFrames, interval: number = 1) {
    const { year, month, today, parsedDate } = this.parseJsDate(new Date());
    const parsedIsoString = parsedDate.toISOString();
    switch (timeframe) {
      case 'month': {
        return { lt: parsedIsoString, gt: new Date(year, parsedDate.getMonth() - interval).toISOString() };
      }
      case 'week': {
        const weekStartDate = new Date(parsedDate);
        weekStartDate.setDate(weekStartDate.getDate() - 6 * interval);
        const weekStart = this.parseJsDate(weekStartDate);
        return {
          lt: parsedIsoString,
          gt: new Date(weekStart.year, weekStart.month, weekStart.today).toISOString()
        };
      }
      case 'hour': {
        // Get the last hour from the current time
        const previousHour = parsedDate.getHours() - interval;
        return {
          lt: new Date(year, month, today, parsedDate.getHours()).toISOString(),
          gt: new Date(year, month, today, previousHour).toISOString()
        };
      }
      default: {
        return {
          lt: parsedIsoString,
          gt: new Date(year, month, today - interval).toISOString()
        };
      }
    }
  }

  private timeFrameDeleteQuery(timeframe: event_types.TimeFrames, interval: number = 1) {
    const { year, month, today, parsedDate } = this.parseJsDate(new Date());
    switch (timeframe) {
      case 'month': {
        return { lt: new Date(year, parsedDate.getMonth() - interval).toISOString() };
      }
      case 'week': {
        const weekStartDate = new Date(parsedDate);
        weekStartDate.setDate(weekStartDate.getDate() - 6 * interval);
        const { month, year, today } = this.parseJsDate(weekStartDate);
        return {
          lt: new Date(year, month, today).toISOString()
        };
      }
      case 'hour': {
        const previousHour = parsedDate.getHours() - interval;
        return {
          lt: new Date(year, month, today, previousHour).toISOString()
        };
      }
      default: {
        return {
          lt: new Date(year, month, today - interval).toISOString()
        };
      }
    }
  }

  private mapListCurrentConnectionsResponse(result: SdkReportingDecoded | null): ListCurrentConnections {
    if (!result) {
      return {
        users: 0,
        sdks: []
      };
    }
    return {
      users: Number(result.users),
      sdks: result.sdks?.data || []
    };
  }
  private async listConnectionsQuery(data: event_types.ListCurrentConnectionsRequest) {
    const { range } = data;
    if (!range) {
      return this.db.sql`
        WITH
          filtered AS (
            SELECT
              *
            FROM
              sdk_report_events
            WHERE
              disconnect_at IS NULL
              AND jwt_exp > NOW()
          ),
          unique_users AS (
            SELECT
              COUNT(DISTINCT user_id) AS count
            FROM
              filtered
          ),
          sdk_versions_array AS (
            SELECT
              sdk,
              COUNT(DISTINCT client_id) AS clients,
              COUNT(DISTINCT user_id) AS users
            FROM
              filtered
            GROUP BY
              sdk
          )
        SELECT
          (
            SELECT
              COALESCE(count, 0)
            FROM
              unique_users
          ) AS users,
          (
            SELECT
              JSON_AGG(ROW_TO_JSON(s))
            FROM
              sdk_versions_array s
          ) AS sdks;
      `
        .decoded(SdkReporting)
        .first();
    }
    const endDate = data.range?.end_date ? new Date(data.range.end_date) : new Date();
    const startDate = new Date(range.start_date);
    const lt = endDate.toISOString();
    const gt = startDate.toISOString();
    return await this.db.sql`
      WITH
        filtered AS (
          SELECT
            *
          FROM
            sdk_report_events
          WHERE
            disconnect_at IS NULL
            AND jwt_exp > NOW()
            AND connect_at > ${{ type: 1184, value: gt }}
            AND connect_at <= ${{ type: 1184, value: lt }}
        ),
        unique_users AS (
          SELECT
            COUNT(DISTINCT user_id) AS count
          FROM
            filtered
        ),
        sdk_versions_array AS (
          SELECT
            sdk,
            COUNT(DISTINCT client_id) AS clients,
            COUNT(DISTINCT user_id) AS users
          FROM
            filtered
          GROUP BY
            sdk
        )
      SELECT
        (
          SELECT
            COALESCE(count, 0)
          FROM
            unique_users
        ) AS users,
        (
          SELECT
            JSON_AGG(ROW_TO_JSON(s))
          FROM
            sdk_versions_array s
        ) AS sdks;
    `
      .decoded(SdkReporting)
      .first();
  }

  private updateTableFilter() {
    const { year, month, today } = this.parseJsDate(new Date());
    const nextDay = today + 1;
    return {
      gte: new Date(year, month, today).toISOString(),
      lt: new Date(year, month, nextDay).toISOString()
    };
  }

  async reportSdkConnect(data: SdkConnectBucketData): Promise<void> {
    const { sdk, connect_at, user_id, user_agent, jwt_exp, client_id } = data;
    const connectIsoString = connect_at.toISOString();
    const jwtExpIsoString = jwt_exp!.toISOString();
    const { gte, lt } = this.updateTableFilter();
    const uuid = v4();
    const result = await this.db.sql`
      UPDATE sdk_report_events
      SET
        connect_at = ${{ type: 1184, value: connectIsoString }},
        sdk = ${{ type: 'varchar', value: sdk }},
        user_agent = ${{ type: 'varchar', value: user_agent }},
        jwt_exp = ${{ type: 1184, value: jwtExpIsoString }},
        disconnect_at = NULL
      WHERE
        user_id = ${{ type: 'varchar', value: user_id }}
        AND client_id = ${{ type: 'varchar', value: client_id }}
        AND connect_at >= ${{ type: 1184, value: gte }}
        AND connect_at < ${{ type: 1184, value: lt }};
    `.rows();
    if (result.length === 0) {
      await this.db.sql`
        INSERT INTO
          sdk_report_events (
            user_id,
            client_id,
            connect_at,
            sdk,
            user_agent,
            jwt_exp,
            id
          )
        SELECT
          ${{ type: 'varchar', value: user_id }},
          ${{ type: 'varchar', value: client_id }},
          ${{ type: 1184, value: connectIsoString }},
          ${{ type: 'varchar', value: sdk }},
          ${{ type: 'varchar', value: user_agent }},
          ${{ type: 1184, value: jwtExpIsoString }},
          ${{ type: 'varchar', value: uuid }}
        WHERE
          NOT EXISTS (
            SELECT
              1
            FROM
              sdk_report_events
            WHERE
              user_id = ${{ type: 'varchar', value: user_id }}
              AND client_id = ${{ type: 'varchar', value: client_id }}
              AND connect_at >= ${{ type: 1184, value: gte }}
              AND connect_at < ${{ type: 1184, value: lt }}
          );
      `.execute();
    }
  }
  async reportSdkDisconnect(data: SdkDisconnectEventData): Promise<void> {
    const { user_id, client_id, disconnect_at } = data;
    const disconnectIsoString = disconnect_at.toISOString();
    const { gte, lt } = this.updateTableFilter();
    await this.db.sql`
      UPDATE sdk_report_events
      SET
        disconnect_at = ${{ type: 1184, value: disconnectIsoString }},
        jwt_exp = NULL
      WHERE
        user_id = ${{ type: 'varchar', value: user_id }}
        AND client_id = ${{ type: 'varchar', value: client_id }}
        AND connect_at >= ${{ type: 1184, value: gte }}
        AND connect_at < ${{ type: 1184, value: lt }};
    `.execute();
  }
  async listCurrentConnections(data: ListCurrentConnectionsRequest): Promise<ListCurrentConnections> {
    const result = await this.listConnectionsQuery(data);
    return this.mapListCurrentConnectionsResponse(result);
  }

  async scrapeSdkData(data: ScrapeSdkDataRequest): Promise<ListCurrentConnections> {
    const { timeframe, interval } = data;
    const { lt, gt } = this.timeFrameQuery(timeframe, interval);
    const result = await this.db.sql`
      WITH
        filtered AS (
          SELECT
            *
          FROM
            sdk_report_events
          WHERE
            connect_at > ${{ type: 1184, value: gt }}
            AND connect_at <= ${{ type: 1184, value: lt }}
        ),
        unique_users AS (
          SELECT
            COUNT(DISTINCT user_id) AS count
          FROM
            filtered
        ),
        sdk_versions_array AS (
          SELECT
            sdk,
            COUNT(DISTINCT client_id) AS clients,
            COUNT(DISTINCT user_id) AS users
          FROM
            filtered
          GROUP BY
            sdk
        )
      SELECT
        (
          SELECT
            COALESCE(count, 0)
          FROM
            unique_users
        ) AS users,
        (
          SELECT
            JSON_AGG(ROW_TO_JSON(s))
          FROM
            sdk_versions_array s
        ) AS sdks;
    `
      .decoded(SdkReporting)
      .first();
    return this.mapListCurrentConnectionsResponse(result);
  }
  async deleteOldSdkData(data: DeleteOldSdkData): Promise<void> {
    const { timeframe, interval } = data;
    const { lt } = this.timeFrameDeleteQuery(timeframe, interval);
    const result = await this.db.sql`
      DELETE FROM sdk_report_events
      WHERE
        connect_at < ${{ type: 1184, value: lt }}
        AND (
          disconnect_at IS NOT NULL
          OR (
            jwt_exp < NOW()
            AND disconnect_at IS NULL
          )
        );
    `.execute();
    console.log(result);
  }

  async [Symbol.asyncDispose]() {
    await this.db[Symbol.asyncDispose]();
  }

  async prepareStatements(connection: pg_wire.PgConnection) {
    // It should be possible to prepare statements for some common operations here.
    // This has not been implemented yet.
  }
}
