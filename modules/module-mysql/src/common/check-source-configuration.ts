import mysqlPromise from 'mysql2/promise';
import * as mysql_utils from '../utils/mysql-utils.js';

const MIN_SUPPORTED_VERSION = '5.7.0';

export async function checkSourceConfiguration(connection: mysqlPromise.Connection): Promise<string[]> {
  const errors: string[] = [];

  const version = await mysql_utils.getMySQLVersion(connection);
  if (!mysql_utils.isVersionAtLeast(version, MIN_SUPPORTED_VERSION)) {
    errors.push(`MySQL versions older than ${MIN_SUPPORTED_VERSION} are not supported. Your version is: ${version}.`);
  }

  const [[result]] = await mysql_utils.retriedQuery({
    connection,
    query: `
      SELECT
        @@GLOBAL.gtid_mode AS gtid_mode,
        @@GLOBAL.log_bin AS log_bin,
        @@GLOBAL.server_id AS server_id,
        @@GLOBAL.log_bin_basename AS binlog_file,
        @@GLOBAL.log_bin_index AS binlog_index_file
      `
  });

  if (result.gtid_mode != 'ON') {
    errors.push(`GTID is not enabled, it is currently set to ${result.gtid_mode}. Please enable it.`);
  }

  if (result.log_bin != 1) {
    errors.push('Binary logging is not enabled. Please enable it.');
  }

  if (result.server_id < 0) {
    errors.push(
      `Your Server ID setting is too low, it must be greater than 0. It is currently ${result.server_id}. Please correct your configuration.`
    );
  }

  if (!result.binlog_file) {
    errors.push('Binary log file is not set. Please check your settings.');
  }

  if (!result.binlog_index_file) {
    errors.push('Binary log index file is not set. Please check your settings.');
  }

  // Ensure ROW format
  const [[binLogFormatResult]] = await mysql_utils.retriedQuery({
    connection,
    query: `SHOW VARIABLES LIKE 'binlog_format';`
  });

  if (binLogFormatResult.Value !== 'ROW') {
    errors.push('Binary log format must be set to "ROW". Please correct your configuration.');
  }

  // Ensure full row image for row-based events
  const [[rowImageResult]] = await mysql_utils.retriedQuery({
    connection,
    query: `SHOW GLOBAL VARIABLES LIKE 'binlog_row_image';`
  });

  if (rowImageResult.Value.toUpperCase() !== 'FULL') {
    errors.push(
      `binlog_row_image must be set to "FULL" globally. Current value: ${rowImageResult.Value ?? 'UNKNOWN'}. ` +
        `Set 'binlog_row_image=FULL' in the MySQL configuration and restart the server. ` +
        `Note: session-level overrides are possible and not detected by this check.`
    );
  }

  return errors;
}
