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

export type PostgresReportStorageOptions = {
  config: NormalizedPostgresStorageConfig;
};

export class PostgresReportStorageFactory implements storage.ReportStorage {
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
      gte: new Date(year, month, today).toISOString(),
      lt: new Date(year, month, nextDay).toISOString()
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
