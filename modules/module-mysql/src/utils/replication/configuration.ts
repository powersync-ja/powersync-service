import mysql from 'mysql2/promise';
import { retriedQuery } from '../mysql_utils.js';

export async function checkSourceConfiguration(db: mysql.Pool) {
  const errors: string[] = [];
  const [[result]] = await retriedQuery({
    db,
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

  const [[binLogFormatResult]] = await retriedQuery({
    db,
    query: `SHOW VARIABLES LIKE 'binlog_format';`
  });

  if (binLogFormatResult.Value !== 'ROW') {
    errors.push('Binary log format must be set to "ROW". Please correct your configuration');
  }

  return errors;
}
