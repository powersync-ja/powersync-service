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

  private listConnectionsDateRangeQuery(data: event_types.ListCurrentConnectionsRequest) {
    const { range } = data;
    if (!range) {
      const query = `
        WITH filtered AS (
              SELECT *
              FROM sdk_report_events
              WHERE disconnect_at IS NULL
              AND jwt_exp > NOW()
                        ),
        unique_users AS (
              SELECT COUNT(DISTINCT user_id) AS count
              FROM filtered
                        ),
        sdk_versions_array AS (
              SELECT sdk,
              COUNT(*) AS total,
              COUNT(DISTINCT client_id) AS clients,
              COUNT(DISTINCT user_id) AS users
              FROM filtered
              GROUP BY sdk
                            )
        SELECT
          COALESCE(u.count, 0) AS users,
          JSON_AGG(ROW_TO_JSON(s)) AS sdks
        FROM unique_users u
        JOIN sdk_versions_array s ON TRUE;
    `;
      return {
        statement: query
      };
    }
    const endDate = data.range?.end_date ? new Date(data.range.end_date) : new Date();
    const startDate = new Date(range.start_date);
    const query = `
      WITH filtered AS (
              SELECT *
              FROM sdk_report_events
              WHERE disconnect_at IS NULL
              AND jwt_exp > NOW()
              AND connect_at > CAST($1 AS TIMESTAMP WITH TIME ZONE)
              AND connect_at <= CAST($2 AS TIMESTAMP WITH TIME ZONE)
      ),
      unique_users AS (
              SELECT COUNT(DISTINCT user_id) AS count
              FROM filtered
                        ),
      sdk_versions_array AS (
              SELECT sdk,
              COUNT(*) AS total,
              COUNT(DISTINCT client_id) AS clients,
              COUNT(DISTINCT user_id) AS users
              FROM filtered
              GROUP BY sdk
                            )
      SELECT COALESCE(u.count, 0) AS users, JSON_AGG(ROW_TO_JSON(s)) AS sdks
      FROM unique_users u
      JOIN sdk_versions_array s ON TRUE;
    `;
    const lt = endDate.toISOString();
    const gt = startDate.toISOString();
    return { statement: query, params: [{ value: gt }, { value: lt }] };
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
    const query = `
    INSERT INTO sdk_report_events (user_id, client_id, connect_at, sdk, user_agent, jwt_exp, id)
    VALUES ($1, $2, $3, $4, $5, $6, $7)
    ON CONFLICT (user_id, client_id, connect_at)
    DO UPDATE SET
      connect_at = CAST($3 AS TIMESTAMP WITH TIME ZONE),
      sdk = $4,
      user_agent = $5,
      jwt_exp = CAST($6 AS TIMESTAMP WITH TIME ZONE),
      disconnect_at = NULL
    WHERE sdk_report_events.connect_at >= CAST($8 AS TIMESTAMP WITH TIME ZONE)
      AND sdk_report_events.connect_at < CAST($9 AS TIMESTAMP WITH TIME ZONE);`;
    try {
      const result = await this.db.query({
        statement: query,
        params: [
          { type: 'varchar', value: user_id },
          { type: 'varchar', value: client_id },
          { type: 1184, value: connectIsoString },
          { type: 'varchar', value: sdk },
          { type: 'varchar', value: user_agent },
          { type: 1184, value: jwtExpIsoString },
          { type: 'varchar', value: v4() },
          { type: 1184, value: gte },
          { type: 1184, value: lt }
        ]
      });
      console.log(result.rows);
    } catch (error) {
      console.log(error);
    }
  }
  async reportSdkDisconnect(data: SdkDisconnectEventData): Promise<void> {
    const { user_id, client_id, disconnect_at } = data;
    const disconnectIsoString = disconnect_at.toISOString();
    const { gte, lt } = this.updateTableFilter();
    const query = `
      UPDATE sdk_report_events
      SET
        disconnect_at = CAST($1 AS TIMESTAMP WITH TIME ZONE),
        jwt_exp = NULL
      WHERE user_id = $2
        AND client_id = $3
        AND connect_at >= CAST($4 AS TIMESTAMP WITH TIME ZONE)
        AND connect_at < CAST($5 AS TIMESTAMP WITH TIME ZONE);`;

    try {
      const result = await this.db.query({
        statement: query,
        params: [
          { type: 1184, value: disconnectIsoString },
          { type: 'varchar', value: user_id },
          { type: 'varchar', value: client_id },
          { type: 1184, value: gte },
          { type: 1184, value: lt }
        ]
      });
      console.log(result.rows);
    } catch (error) {
      console.log(error);
    }
  }
  async listCurrentConnections(data: ListCurrentConnectionsRequest): Promise<ListCurrentConnections> {
    const statement = this.listConnectionsDateRangeQuery(data);
    const result = await this.db.query(statement);
    console.log(result.rows);
    return {
      users: 0,
      sdks: []
    };
  }

  async scrapeSdkData(data: ScrapeSdkDataRequest): Promise<ListCurrentConnections> {
    const { timeframe, interval } = data;
    const { lt, gt } = this.timeFrameQuery(timeframe, interval);
    const query = `
      WITH filtered AS (
            SELECT *
            FROM sdk_report_events
            WHERE connect_at > CAST($1 AS TIMESTAMP WITH TIME ZONE)
            AND connect_at <= CAST($2 AS TIMESTAMP WITH TIME ZONE)
                        ),
      unique_users AS (
            SELECT COUNT(DISTINCT user_id) AS count
            FROM filtered
                        ),
      sdk_versions_array AS (
            SELECT sdk,
            COUNT(*) AS total,
            COUNT(DISTINCT client_id) AS clients,
            COUNT(DISTINCT user_id) AS users
            FROM filtered
            GROUP BY sdk
                            )
      SELECT COALESCE(u.count, 0) AS users, JSON_AGG(ROW_TO_JSON(s)) AS sdks
      FROM unique_users u
      JOIN sdk_versions_array s ON TRUE;
    `;
    const result = await this.db.query({ statement: query, params: [{ value: gt }, { value: lt }] });
    console.log(result.rows);
    return {
      users: 0,
      sdks: []
    };
  }
  async deleteOldSdkData(data: DeleteOldSdkData): Promise<void> {
    const { timeframe, interval } = data;
    const { lt } = this.timeFrameDeleteQuery(timeframe, interval);
    const query = `
    DELETE FROM sdk_report_events
    WHERE connect_at < CAST($1 AS TIMESTAMP WITH TIME ZONE)
      AND (
        disconnect_at IS NOT NULL
        OR (jwt_exp < NOW() AND disconnect_at IS NULL)
          );
`;
    const params = [{ value: lt }];
    const result = await this.db.query({ statement: query, params });
    console.log(result.rows);
  }

  async [Symbol.asyncDispose]() {
    await this.db[Symbol.asyncDispose]();
  }

  async prepareStatements(connection: pg_wire.PgConnection) {
    // It should be possible to prepare statements for some common operations here.
    // This has not been implemented yet.
  }
}
