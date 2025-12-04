import { BaseObserver, logger } from '@powersync/lib-services-framework';
import sql from 'mssql';
import { NormalizedMSSQLConnectionConfig } from '../types/types.js';
import { POWERSYNC_VERSION } from '@powersync/service-core';
import { MSSQLParameter } from '../types/mssql-data-types.js';
import { addParameters } from '../utils/mssql.js';

export const DEFAULT_SCHEMA = 'dbo';

export interface MSSQLConnectionManagerListener {
  onEnded(): void;
}

export class MSSQLConnectionManager extends BaseObserver<MSSQLConnectionManagerListener> {
  private readonly pool: sql.ConnectionPool;

  constructor(
    public options: NormalizedMSSQLConnectionConfig,
    poolOptions: sql.PoolOpts<sql.Connection>
  ) {
    super();
    // The pool is lazy - no connections are opened until a query is performed.
    this.pool = new sql.ConnectionPool({
      authentication: options.authentication,
      user: options.username,
      password: options.password,
      server: options.hostname,
      port: options.port,
      database: options.database,
      pool: poolOptions,
      options: {
        appName: `powersync/${POWERSYNC_VERSION}`,
        encrypt: true, // Required for Azure
        trustServerCertificate: options.additionalConfig.trustServerCertificate
      }
    });
  }

  public get connectionTag() {
    return this.options.tag;
  }

  public get connectionId() {
    return this.options.id;
  }

  public get databaseName() {
    return this.options.database;
  }

  public get schema() {
    return this.options.schema ?? DEFAULT_SCHEMA;
  }

  private async ensureConnected(): Promise<void> {
    await this.pool.connect();
  }

  async createTransaction(): Promise<sql.Transaction> {
    await this.ensureConnected();
    return this.pool.transaction();
  }

  async createRequest(): Promise<sql.Request> {
    await this.ensureConnected();
    return this.pool.request();
  }

  async query(query: string, parameters?: MSSQLParameter[]): Promise<sql.IResult<any>> {
    await this.ensureConnected();
    for (let tries = 2; ; tries--) {
      try {
        logger.debug(`Executing query: ${query}`);
        let request = this.pool.request();
        if (parameters) {
          request = addParameters(request, parameters);
        }
        return await request.query(query);
      } catch (e) {
        if (tries == 1) {
          throw e;
        }
        logger.warn('Query error, retrying..', e);
      }
    }
  }

  async execute(procedure: string, parameters?: MSSQLParameter[]): Promise<sql.IProcedureResult<any>> {
    await this.ensureConnected();
    let request = this.pool.request();
    if (parameters) {
      if (parameters) {
        request = addParameters(request, parameters);
      }
    }
    return request.execute(procedure);
  }

  async end(): Promise<void> {
    if (this.pool.connected) {
      try {
        await this.pool.close();
      } catch (error) {
        // We don't particularly care if any errors are thrown when shutting down the pool
        logger.warn('Error shutting down MSSQL connection pool', error);
      } finally {
        this.iterateListeners((listener) => {
          listener.onEnded?.();
        });
      }
    }
  }
}
