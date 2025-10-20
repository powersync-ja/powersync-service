import sql from 'mssql';
import { SourceTable } from '@powersync/service-core';
import { coerce, gte } from 'semver';
import { logger } from '@powersync/lib-services-framework';
import { MSSQLConnectionManager } from '../replication/MSSQLConnectionManager.js';
import { LSN } from '../common/LSN.js';
import { CaptureInstance, MSSQLSourceTable } from '../common/MSSQLSourceTable.js';
import { MSSQLParameter } from '../types/mssql-data-types.js';

export interface CreateStreamingQueryOptions {
  query: string;
  // Request to create the streaming query from
  request: sql.Request;
  // Cancel the iteration if this signal is aborted
  signal?: AbortSignal;
  // Maximum number of rows to buffer before pausing the request
  maxQueueSize?: number;
}

export interface StreamingQuery {
  columns: { [name: string]: sql.IColumn };
  [Symbol.asyncIterator](): AsyncIterator<Record<string, unknown>>;
}

export async function createStreamingQuery(options: CreateStreamingQueryOptions): Promise<StreamingQuery> {
  const { query, request, signal } = options;
  const maxQueueSize = options.maxQueueSize ?? 1000;

  // Wait for the recordSet event before returning
  let columns: { [name: string]: sql.IColumn } = await new Promise((resolve) => {
    // Record Column metadata
    request.on('recordSet', (recordSet: { [name: string]: sql.IColumn }) => {
      columns = recordSet;
      resolve(recordSet);
    });
  });

  async function* rowGenerator(): AsyncGenerator<Record<string, unknown>> {
    const rowQueue: Array<Record<string, unknown>> = [];
    let resolveNext: (() => void) | null = null;
    let streamingError: Error | null = null;
    let isPaused = false;
    let isDone = false;

    try {
      request.on('row', (row: Record<string, unknown>) => {
        rowQueue.push(row);
        if (rowQueue.length >= maxQueueSize) {
          request.pause();
          isPaused = true;
        }
        if (resolveNext) {
          resolveNext();
          resolveNext = null;
        }
      });

      request.on('done', () => {
        isDone = true;
        if (resolveNext) {
          resolveNext();
          resolveNext = null;
        }
      });

      request.on('error', (err) => {
        streamingError = err;
        isDone = true;
      });

      // Don't start the query if we are already aborted
      if (signal && signal.aborted) {
        isDone = true;
      } else {
        // Start streaming
        request.query(query);

        // Handle aborts by cancelling the request
        signal?.addEventListener(
          'abort',
          () => {
            isDone = true;
            request.cancel();
            if (resolveNext) {
              resolveNext();
              resolveNext = null;
            }
          },
          { once: true }
        );
      }

      // Loop until the stream is done and the queue is empty
      while (!isDone || rowQueue.length > 0) {
        if (rowQueue.length > 0) {
          yield rowQueue.shift() as Record<string, unknown>;
          // Resume streaming if we are below half the max queue size
          if (isPaused && rowQueue.length <= maxQueueSize / 2) {
            request.resume();
          }
        } else if (!isDone) {
          await new Promise<void>((resolve) => {
            resolveNext = resolve;
          });
        }
      }

      if (streamingError) {
        throw streamingError;
      }
    } finally {
      request.cancel();
    }
  }

  return {
    columns: columns,
    [Symbol.asyncIterator]: rowGenerator
  };
}

export const SUPPORTED_ENGINE_EDITIONS = new Map([
  [2, 'Standard'],
  [3, 'Enterprise - Enterprise, Developer, Evaluation'],
  [5, 'SqlDatabase - Azure SQL Database'],
  [8, 'SqlManagedInstance - Azure SQL Managed Instance']
]);

// SQL Server 2022 and newer
export const MINIMUM_SUPPORTED_VERSION = '16.0';

export async function checkSourceConfiguration(connectionManager: MSSQLConnectionManager): Promise<string[]> {
  const errors: string[] = [];
  // 1) Check MSSQL version and Editions
  const { recordset: versionResult } = await connectionManager.query(`
      SELECT
        CAST(SERVERPROPERTY('EngineEdition') AS int)           AS engine,
        CAST(SERVERPROPERTY('Edition') AS nvarchar(128))       AS edition,
        CAST(SERVERPROPERTY('ProductVersion') AS nvarchar(128)) AS version
    `);

  // If the edition is unsupported, return immediately
  if (!SUPPORTED_ENGINE_EDITIONS.has(versionResult[0]?.engine)) {
    errors.push(
      `The SQL Server edition '${versionResult[0]?.edition}' is not supported. PowerSync requires a MSSQL edition that supports CDC: ${Array.from(
        SUPPORTED_ENGINE_EDITIONS.values()
      ).join(', ')}.`
    );
    return errors;
  }

  // Only applicable to SQL Server stand-alone editions
  if (versionResult[0]?.engine == 2 || versionResult[0]?.engine == 3) {
    if (!isVersionAtLeast(versionResult[0]?.version, MINIMUM_SUPPORTED_VERSION)) {
      errors.push(
        `The SQL Server version '${versionResult[0]?.version}' is not supported. PowerSync requires MSSQL 2022 (v16) or newer.`
      );
    }
  }

  // 2) Check DB-level CDC
  const { recordset: cdcEnabledResult } = await connectionManager.query(`
      SELECT name AS db_name, is_cdc_enabled FROM sys.databases WHERE name = DB_NAME();
    `);
  const cdcEnabled = cdcEnabledResult[0]?.is_cdc_enabled;

  if (!cdcEnabled) {
    errors.push(`CDC is not enabled for database. Please enable it.`);
  }

  // 3) Check CDC user permissions
  const { recordset: cdcUserResult } = await connectionManager.query(`
      SELECT
        CASE
          WHEN IS_SRVROLEMEMBER('sysadmin') = 1
            OR IS_MEMBER('db_owner') = 1
            OR IS_MEMBER('cdc_admin') = 1
            OR IS_MEMBER('cdc_reader') = 1
            THEN 1 ELSE 0
          END AS has_cdc_access;
    `);

  if (!cdcUserResult[0]?.has_cdc_access) {
    errors.push(`The current user does not have the 'cdc_reader' role. Please assign this role to the user.`);
  }

  return errors;
}

export interface IsTableEnabledForCDCOptions {
  connectionManager: MSSQLConnectionManager;
  table: string;
  schema: string;
}
/**
 *  Check if the specified table is enabled for CDC.
 *  @param options
 */
export async function isTableEnabledForCDC(options: IsTableEnabledForCDCOptions): Promise<boolean> {
  const { connectionManager, table, schema } = options;

  const { recordset: checkResult } = await connectionManager.query(
    `
      SELECT 1 FROM cdc.change_tables ct
         JOIN sys.tables    AS tbl ON tbl.object_id = ct.source_object_id
         JOIN sys.schemas   AS sch ON sch.schema_id = tbl.schema_id
      WHERE sch.name = '${schema}'
        AND tbl.name = '${table}'
      `
  );
  return checkResult.length > 0;
}

/**
 *  Check if the supplied version is newer or equal to the target version.
 *  @param version
 *  @param minimumVersion
 */
export function isVersionAtLeast(version: string, minimumVersion: string): boolean {
  const coercedVersion = coerce(version);
  const coercedMinimumVersion = coerce(minimumVersion);

  return gte(coercedVersion!, coercedMinimumVersion!, { loose: true });
}

export interface IsWithinRetentionThresholdOptions {
  checkpointLSN: LSN;
  tables: MSSQLSourceTable[];
  connectionManager: MSSQLConnectionManager;
}

/**
 *  Checks that CDC the specified checkpoint LSN is within the retention threshold for all specified tables.
 *  CDC periodically cleans up old data up to the retention threshold. If replication has been stopped for too long it is
 *  possible for the checkpoint LSN to be older than the minimum LSN in the CDC tables. In such a case we need to perform a new snapshot.
 *  @param options
 */
export async function isWithinRetentionThreshold(options: IsWithinRetentionThresholdOptions): Promise<boolean> {
  const { checkpointLSN, tables, connectionManager } = options;
  for (const table of tables) {
    const { recordset: result } = await connectionManager.query('SELECT sys.fn_cdc_get_min_lsn(dbo_lists) AS min_lsn', [
      {
        name: 'capture_instance',
        type: sql.NVarChar,
        value: table.captureInstance
      }
    ]);

    const rawMinLSN: Buffer = result[0].min_lsn;
    const minLSN = LSN.fromBinary(rawMinLSN);
    if (minLSN > checkpointLSN) {
      logger.warn(
        `The checkpoint LSN:[${checkpointLSN}] is older than the minimum LSN:[${minLSN}] for table ${table.sourceTable.qualifiedName}. This indicates that the checkpoint LSN is outside of the retention window.`
      );
      return false;
    }
  }
  return true;
}

export async function getCaptureInstance(
  connectionManager: MSSQLConnectionManager,
  table: SourceTable
): Promise<CaptureInstance | null> {
  const { recordset: result } = await connectionManager.query(
    `
      SELECT
        ct.capture_instance,
        OBJECT_SCHEMA_NAME(ct.[object_id]) AS cdc_schema
      FROM
        sys.tables tbl
          INNER JOIN sys.schemas sch ON tbl.schema_id = sch.schema_id
          INNER JOIN cdc.change_tables ct ON ct.source_object_id = tbl.object_id
      WHERE sch.name = '${table.schema}'
        AND tbl.name = '${table.name}'
        AND ct.end_lsn IS NULL;
      `
  );

  if (result.length === 0) {
    return null;
  }

  return {
    name: result[0].capture_instance,
    schema: result[0].cdc_schema
  };
}

/**
 *  Return the maximum LSN in the CDC tables. This is the LSN that corresponds to the latest update available.
 *  @param connectionManager
 */
export async function getLatestLSN(connectionManager: MSSQLConnectionManager): Promise<LSN> {
  const { recordset: result } = await connectionManager.query('SELECT sys.fn_cdc_get_max_lsn() AS max_lsn;');
  // LSN is a binary(10) returned as a Buffer
  const rawLSN: Buffer = result[0].max_lsn;
  return LSN.fromBinary(rawLSN);
}

/**
 *  Escapes an identifier for use in MSSQL queries.
 *  @param identifier
 */
export function escapeIdentifier(identifier: string): string {
  return `[${identifier}]`;
}

export function isIColumnMetadata(obj: any): obj is sql.IColumnMetadata {
  if (obj === null || typeof obj !== 'object' || Array.isArray(obj)) {
    return false;
  }

  let propertiesMatched = true;
  for (const value of Object.values(obj)) {
    const property = value as any;
    propertiesMatched =
      typeof property.index === 'number' &&
      typeof property.name === 'string' &&
      typeof property.length === 'number' &&
      (typeof property.type === 'function' || typeof property.type === 'object') &&
      typeof property.nullable === 'boolean' &&
      typeof property.caseSensitive === 'boolean' &&
      typeof property.identity === 'boolean' &&
      typeof property.readOnly === 'boolean';
  }

  return propertiesMatched;
}

export function addParameters(request: sql.Request, parameters: MSSQLParameter[]): sql.Request {
  for (const param of parameters) {
    if (param.type) {
      request.input(param.name, param.type, param.value);
    } else {
      request.input(param.name, param.value);
    }
  }
  return request;
}
