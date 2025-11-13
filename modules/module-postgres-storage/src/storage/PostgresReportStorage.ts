import { storage } from '@powersync/service-core';
import * as pg_wire from '@powersync/service-jpgwire';
import { event_types } from '@powersync/service-types';
import { v4 } from 'uuid';
import * as lib_postgres from '@powersync/lib-service-postgres';
import { NormalizedPostgresStorageConfig } from '../types/types.js';
import { SdkReporting, SdkReportingDecoded } from '../types/models/SdkReporting.js';
import { toInteger } from 'ix/util/tointeger.js';
import { logger } from '@powersync/lib-services-framework';
import { getStorageApplicationName } from '../utils/application-name.js';
import { STORAGE_SCHEMA_NAME } from '../utils/db.js';
import { ClientConnectionResponse } from '@powersync/service-types/dist/reports.js';

export type PostgresReportStorageOptions = {
  config: NormalizedPostgresStorageConfig;
};

export class PostgresReportStorage implements storage.ReportStorage {
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

  private mapListCurrentConnectionsResponse(
    result: SdkReportingDecoded | null
  ): event_types.ClientConnectionReportResponse {
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
  private async listConnectionsQuery() {
    return await this.db.sql`
      WITH
        filtered AS (
          SELECT
            *
          FROM
            connection_report_events
          WHERE
            disconnected_at IS NULL
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

  private updateTableFilter() {
    const { year, month, today } = this.parseJsDate(new Date());
    const nextDay = today + 1;
    return {
      gte: new Date(Date.UTC(year, month, today)).toISOString(),
      lt: new Date(Date.UTC(year, month, nextDay)).toISOString()
    };
  }

  private clientsConnectionPagination(params: event_types.ClientConnectionAnalyticsRequest): {
    mainQuery: pg_wire.Statement;
    countQuery: pg_wire.Statement;
  } {
    const { cursor, limit, client_id, user_id, date_range } = params;
    const queryLimit = limit || 100;
    const queryParams: pg_wire.StatementParam[] = [];
    let countQuery = `SELECT COUNT(*) AS total FROM connection_report_events`;
    let query = `SELECT id, user_id, client_id, user_agent, sdk, jwt_exp::text AS jwt_exp, disconnected_at, connected_at::text AS connected_at, disconnected_at::text AS disconnected_at  FROM connection_report_events`;
    let intermediateQuery = '';
    /** Create a user_id/ client_id filter is they exist  */
    if (client_id || user_id) {
      if (client_id && !user_id) {
        intermediateQuery += ` WHERE client_id = $1`;
        queryParams.push({ type: 'varchar', value: client_id });
      } else if (!client_id && user_id) {
        intermediateQuery += ` WHERE user_id = $1`;
        queryParams.push({ type: 'varchar', value: user_id });
      } else {
        intermediateQuery += ' WHERE client_id = $1 AND user_id = $2';
        queryParams.push({ type: 'varchar', value: client_id! });
        queryParams.push({ type: 'varchar', value: user_id! });
      }
    }

    /** Create a date range filter if it exists */
    if (date_range) {
      const { start, end } = date_range;
      intermediateQuery +=
        queryParams.length === 0
          ? ` WHERE connected_at >= $1 AND connected_at <= $2`
          : ` AND connected_at >= $${queryParams.length + 1} AND connected_at <= $${queryParams.length + 2}`;
      queryParams.push({ type: 1184, value: start.toISOString() });
      queryParams.push({ type: 1184, value: end.toISOString() });
    }

    countQuery += intermediateQuery;

    /** Create a cursor filter if it exists. The cursor in postgres is the last item connection date, the id is an uuid so we cant use the same logic as in MongoReportStorage.ts */
    if (cursor) {
      intermediateQuery +=
        queryParams.length === 0 ? ` WHERE connected_at < $1` : ` AND connected_at < $${queryParams.length + 1}`;
      queryParams.push({ type: 1184, value: new Date(cursor).toISOString() });
    }

    /** Order in descending connected at range to match Mongo sort=-1*/
    intermediateQuery += ` ORDER BY connected_at DESC`;
    query += intermediateQuery;

    return {
      mainQuery: {
        statement: query,
        params: queryParams,
        limit: queryLimit
      },
      countQuery: {
        statement: countQuery,
        params: queryParams
      }
    };
  }

  async reportClientConnection(data: event_types.ClientConnectionBucketData): Promise<void> {
    const { sdk, connected_at, user_id, user_agent, jwt_exp, client_id } = data;
    const connectIsoString = connected_at.toISOString();
    const jwtExpIsoString = jwt_exp.toISOString();
    const { gte, lt } = this.updateTableFilter();
    const uuid = v4();
    const result = await this.db.sql`
      UPDATE connection_report_events
      SET
        connected_at = ${{ type: 1184, value: connectIsoString }},
        sdk = ${{ type: 'varchar', value: sdk }},
        user_agent = ${{ type: 'varchar', value: user_agent }},
        jwt_exp = ${{ type: 1184, value: jwtExpIsoString }},
        disconnected_at = NULL
      WHERE
        user_id = ${{ type: 'varchar', value: user_id }}
        AND client_id = ${{ type: 'varchar', value: client_id }}
        AND connected_at >= ${{ type: 1184, value: gte }}
        AND connected_at < ${{ type: 1184, value: lt }};
    `.execute();
    if (result.results[1].status === 'UPDATE 0') {
      await this.db.sql`
        INSERT INTO
          connection_report_events (
            user_id,
            client_id,
            connected_at,
            sdk,
            user_agent,
            jwt_exp,
            id
          )
        VALUES
          (
            ${{ type: 'varchar', value: user_id }},
            ${{ type: 'varchar', value: client_id }},
            ${{ type: 1184, value: connectIsoString }},
            ${{ type: 'varchar', value: sdk }},
            ${{ type: 'varchar', value: user_agent }},
            ${{ type: 1184, value: jwtExpIsoString }},
            ${{ type: 'varchar', value: uuid }}
          )
      `.execute();
    }
  }
  async reportClientDisconnection(data: event_types.ClientDisconnectionEventData): Promise<void> {
    const { user_id, client_id, disconnected_at, connected_at } = data;
    const disconnectIsoString = disconnected_at.toISOString();
    const connectIsoString = connected_at.toISOString();
    await this.db.sql`
      UPDATE connection_report_events
      SET
        disconnected_at = ${{ type: 1184, value: disconnectIsoString }},
        jwt_exp = NULL
      WHERE
        user_id = ${{ type: 'varchar', value: user_id }}
        AND client_id = ${{ type: 'varchar', value: client_id }}
        AND connected_at = ${{ type: 1184, value: connectIsoString }}
    `.execute();
  }
  async getConnectedClients(): Promise<event_types.ClientConnectionReportResponse> {
    const result = await this.listConnectionsQuery();
    return this.mapListCurrentConnectionsResponse(result);
  }

  async getClientConnectionReports(
    data: event_types.ClientConnectionReportRequest
  ): Promise<event_types.ClientConnectionReportResponse> {
    const { start, end } = data;
    const result = await this.db.sql`
      WITH
        filtered AS (
          SELECT
            *
          FROM
            connection_report_events
          WHERE
            connected_at >= ${{ type: 1184, value: start.toISOString() }}
            AND connected_at <= ${{ type: 1184, value: end.toISOString() }}
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

  async getGeneralClientConnectionAnalytics(
    data: event_types.ClientConnectionAnalyticsRequest
  ): Promise<event_types.PaginatedResponse<event_types.ClientConnection>> {
    const limit = data.limit || 100;
    const statement = this.clientsConnectionPagination(data);

    const result = await this.db.queryRows<ClientConnectionResponse>(statement.mainQuery);
    const items = result.map((item) => ({
      ...item,
      /** JS Date conversion to match document schema used for Mongo storage */
      connected_at: new Date(item.connected_at),
      disconnected_at: item.disconnected_at ? new Date(item.disconnected_at) : undefined,
      jwt_exp: item.jwt_exp ? new Date(item.jwt_exp) : undefined
      /** */
    }));
    const count = items.length;
    /** The returned total has been defaulted to 0 due to the overhead using documentCount from the mogo driver this is just to keep consistency with Mongo implementation.
     * cursor.count has been deprecated.
     * */
    return {
      items,
      total: 0,
      /** Setting the cursor to the connected at date of the last item in the list */
      cursor: count === limit ? items[items.length - 1].connected_at.toISOString() : undefined,
      count,
      more: !(count !== limit)
    };
  }

  async deleteOldConnectionData(data: event_types.DeleteOldConnectionData): Promise<void> {
    const { date } = data;
    const result = await this.db.sql`
      DELETE FROM connection_report_events
      WHERE
        connected_at < ${{ type: 1184, value: date.toISOString() }}
        AND (
          disconnected_at IS NOT NULL
          OR (
            jwt_exp < NOW()
            AND disconnected_at IS NULL
          )
        );
    `.execute();
    const deletedRows = toInteger(result.results[1].status.split(' ')[1] || '0');
    if (deletedRows > 0) {
      logger.info(
        `TTL from ${date.toISOString()}: ${deletedRows} PostgresSQL rows have been removed from connection_report_events.`
      );
    }
  }

  async [Symbol.asyncDispose]() {
    await this.db[Symbol.asyncDispose]();
  }

  async prepareStatements(connection: pg_wire.PgConnection) {
    // It should be possible to prepare statements for some common operations here.
    // This has not been implemented yet.
  }
}
