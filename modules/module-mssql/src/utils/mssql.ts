import sql from 'mssql';
import { coerce, gte } from 'semver';
import { logger } from '@powersync/lib-services-framework';
import { MSSQLConnectionManager } from '../replication/MSSQLConnectionManager.js';
import { LSN } from '../common/LSN.js';
import { CaptureInstance, MSSQLSourceTable } from '../common/MSSQLSourceTable.js';
import { MSSQLParameter } from '../types/mssql-data-types.js';
import { SqlSyncRules, TablePattern } from '@powersync/service-sync-rules';
import { getReplicationIdentityColumns, ReplicationIdentityColumnsResult, ResolvedTable } from './schema.js';
import * as service_types from '@powersync/service-types';
import * as sync_rules from '@powersync/service-sync-rules';

export const POWERSYNC_CHECKPOINTS_TABLE = '_powersync_checkpoints';

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

  // 4) Check if the _powersync_checkpoints table is correctly configured
  const checkpointTableErrors = await ensurePowerSyncCheckpointsTable(connectionManager);
  errors.push(...checkpointTableErrors);

  return errors;
}

export async function ensurePowerSyncCheckpointsTable(connectionManager: MSSQLConnectionManager): Promise<string[]> {
  const errors: string[] = [];
  try {
    // check if the dbo_powersync_checkpoints table exists
    const { recordset: checkpointsResult } = await connectionManager.query(`
    SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '${connectionManager.schema}' AND TABLE_NAME = '${POWERSYNC_CHECKPOINTS_TABLE}';
  `);
    if (checkpointsResult.length > 0) {
      // Table already exists, check if CDC is enabled
      const isEnabled = await isTableEnabledForCDC({
        connectionManager,
        table: POWERSYNC_CHECKPOINTS_TABLE,
        schema: connectionManager.schema
      });
      if (!isEnabled) {
        // Enable CDC on the table
        await enableCDCForTable({
          connectionManager,
          table: POWERSYNC_CHECKPOINTS_TABLE
        });
      }
      return errors;
    }
  } catch (error) {
    errors.push(`Failed ensure ${POWERSYNC_CHECKPOINTS_TABLE} table is correctly configured: ${error}`);
  }

  // Try to create the table
  try {
    await connectionManager.query(`
  CREATE TABLE ${connectionManager.schema}.${POWERSYNC_CHECKPOINTS_TABLE} (
    id INT IDENTITY PRIMARY KEY,
    last_updated DATETIME NOT NULL DEFAULT (GETDATE())
  )`);
  } catch (error) {
    errors.push(`Failed to create ${POWERSYNC_CHECKPOINTS_TABLE} table: ${error}`);
  }

  try {
    // Enable CDC on the table if not already enabled
    await enableCDCForTable({
      connectionManager,
      table: POWERSYNC_CHECKPOINTS_TABLE
    });
  } catch (error) {
    errors.push(`Failed to enable CDC on ${POWERSYNC_CHECKPOINTS_TABLE} table: ${error}`);
  }

  return errors;
}

export async function createCheckpoint(connectionManager: MSSQLConnectionManager): Promise<void> {
  await connectionManager.query(`
    MERGE ${connectionManager.schema}.${POWERSYNC_CHECKPOINTS_TABLE} AS target
    USING (SELECT 1 AS id) AS source
    ON target.id = source.id
    WHEN MATCHED THEN 
      UPDATE SET last_updated = GETDATE()
    WHEN NOT MATCHED THEN
      INSERT (last_updated) VALUES (GETDATE());
  `);
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

export interface EnableCDCForTableOptions {
  connectionManager: MSSQLConnectionManager;
  table: string;
}

export async function enableCDCForTable(options: EnableCDCForTableOptions): Promise<void> {
  const { connectionManager, table } = options;

  await connectionManager.execute('sys.sp_cdc_enable_table', [
    { name: 'source_schema', value: connectionManager.schema },
    { name: 'source_name', value: table },
    { name: 'role_name', value: 'NULL' },
    { name: 'supports_net_changes', value: 1 }
  ]);
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
    const minLSN = await getMinLSN(connectionManager, table.captureInstance);
    if (minLSN > checkpointLSN) {
      logger.warn(
        `The checkpoint LSN:[${checkpointLSN}] is older than the minimum LSN:[${minLSN}] for table ${table.sourceTable.qualifiedName}. This indicates that the checkpoint LSN is outside of the retention window.`
      );
      return false;
    }
  }
  return true;
}

export async function getMinLSN(connectionManager: MSSQLConnectionManager, captureInstance: string): Promise<LSN> {
  const { recordset: result } = await connectionManager.query(
    `SELECT sys.fn_cdc_get_min_lsn('${captureInstance}') AS min_lsn`
  );
  const rawMinLSN: Buffer = result[0].min_lsn;
  return LSN.fromBinary(rawMinLSN);
}

export async function incrementLSN(lsn: LSN, connectionManager: MSSQLConnectionManager): Promise<LSN> {
  const { recordset: result } = await connectionManager.query(
    `SELECT sys.fn_cdc_increment_lsn(@lsn) AS incremented_lsn`,
    [{ name: 'lsn', type: sql.VarBinary, value: lsn.toBinary() }]
  );
  return LSN.fromBinary(result[0].incremented_lsn);
}

export interface GetCaptureInstanceOptions {
  connectionManager: MSSQLConnectionManager;
  tableName: string;
  schema: string;
}

export async function getCaptureInstance(options: GetCaptureInstanceOptions): Promise<CaptureInstance | null> {
  const { connectionManager, tableName, schema } = options;
  const { recordset: result } = await connectionManager.query(
    `
      SELECT
        ct.capture_instance,
        OBJECT_SCHEMA_NAME(ct.[object_id]) AS cdc_schema
      FROM
        sys.tables tbl
          INNER JOIN sys.schemas sch ON tbl.schema_id = sch.schema_id
          INNER JOIN cdc.change_tables ct ON ct.source_object_id = tbl.object_id
      WHERE sch.name = '${schema}'
        AND tbl.name = '${tableName}'
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
 *  Return the LSN of the latest transaction recorded in the transaction log
 *  @param connectionManager
 */
export async function getLatestLSN(connectionManager: MSSQLConnectionManager): Promise<LSN> {
  const { recordset: result } = await connectionManager.query(
    'SELECT log_end_lsn FROM sys.dm_db_log_stats(DB_ID()) AS log_end_lsn'
  );
  return LSN.fromString(result[0].log_end_lsn);
}

/**
 *  Return the LSN of the lastest transaction replicated to the CDC tables.
 *  @param connectionManager
 */
export async function getLatestReplicatedLSN(connectionManager: MSSQLConnectionManager): Promise<LSN> {
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

export function toQualifiedTableName(schema: string, tableName: string): string {
  return `${escapeIdentifier(schema)}.${escapeIdentifier(tableName)}`;
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
      (typeof property.length === 'number' || typeof property.length === 'undefined') &&
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

export interface GetDebugTableInfoOptions {
  connectionManager: MSSQLConnectionManager;
  tablePattern: TablePattern;
  table: ResolvedTable;
  syncRules: SqlSyncRules;
}

export async function getDebugTableInfo(options: GetDebugTableInfoOptions): Promise<service_types.TableInfo> {
  const { connectionManager, tablePattern, table, syncRules } = options;
  const { schema } = tablePattern;

  let idColumnsResult: ReplicationIdentityColumnsResult | null = null;
  let idColumnsError: service_types.ReplicationError | null = null;
  try {
    idColumnsResult = await getReplicationIdentityColumns({
      connectionManager: connectionManager,
      schema,
      tableName: table.name
    });
  } catch (ex) {
    idColumnsError = { level: 'fatal', message: ex.message };
  }

  const idColumns = idColumnsResult?.columns ?? [];
  const sourceTable: sync_rules.SourceTableInterface = {
    connectionTag: connectionManager.connectionTag,
    schema: schema,
    name: table.name
  };
  const syncData = syncRules.tableSyncsData(sourceTable);
  const syncParameters = syncRules.tableSyncsParameters(sourceTable);

  if (idColumns.length === 0 && idColumnsError == null) {
    let message = `No replication id found for ${toQualifiedTableName(schema, table.name)}. Replica identity: ${idColumnsResult?.identity}.`;
    if (idColumnsResult?.identity === 'default') {
      message += ' Configure a primary key on the table.';
    }
    idColumnsError = { level: 'fatal', message };
  }

  let selectError: service_types.ReplicationError | null = null;
  try {
    await connectionManager.query(`SELECT TOP 1 * FROM [${toQualifiedTableName(schema, table.name)}]`);
  } catch (e) {
    selectError = { level: 'fatal', message: e.message };
  }

  // Check if CDC is enabled for the table
  let cdcError: service_types.ReplicationError | null = null;
  try {
    const isEnabled = await isTableEnabledForCDC({
      connectionManager: connectionManager,
      table: table.name,
      schema: schema
    });
    if (!isEnabled) {
      cdcError = {
        level: 'fatal',
        message: `CDC is not enabled for table ${toQualifiedTableName(schema, table.name)}. Enable CDC with: sys.sp_cdc_enable_table @source_schema = '${schema}', @source_name = '${table.name}', @role_name = NULL, @supports_net_changes = 1`
      };
    }
  } catch (e) {
    cdcError = { level: 'warning', message: `Could not check CDC status: ${e.message}` };
  }

  // TODO check RLS settings for table

  return {
    schema: schema,
    name: table.name,
    pattern: tablePattern.isWildcard ? tablePattern.tablePattern : undefined,
    replication_id: idColumns.map((c) => c.name),
    data_queries: syncData,
    parameter_queries: syncParameters,
    errors: [idColumnsError, selectError, cdcError].filter((error) => error != null) as service_types.ReplicationError[]
  };
}
