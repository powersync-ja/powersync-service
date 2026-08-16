import { checkSourceConfiguration } from '@module/common/check-source-configuration.js';
import { describe, expect, test } from 'vitest';
import { createMockMySQLConnection } from './util.js';

describe('checkSourceConfiguration', () => {
  test('accepts a primary MySQL server', async () => {
    const { connection, query } = createConnection({ version: '8.4.0', replicaStatuses: [] });

    await expect(checkSourceConfiguration(connection)).resolves.toEqual([]);
    expect(query).toHaveBeenCalledWith('SHOW REPLICA STATUS', []);
  });

  test('rejects a replica on MySQL 8.0.22 and later', async () => {
    const { connection, query } = createConnection({
      version: '8.0.22',
      replicaStatuses: [{ Channel_Name: '' }]
    });

    await expect(checkSourceConfiguration(connection)).resolves.toContain(
      'Connecting PowerSync to a MySQL replica is not supported. Please connect PowerSync directly to the primary server.'
    );
    expect(query).toHaveBeenCalledWith('SHOW REPLICA STATUS', []);
    expect(query).not.toHaveBeenCalledWith('SHOW SLAVE STATUS', []);
  });

  test('uses legacy replica-status syntax before MySQL 8.0.22', async () => {
    const { connection, query } = createConnection({
      version: '5.7.44',
      replicaStatuses: [{ Channel_Name: '' }]
    });

    await expect(checkSourceConfiguration(connection)).resolves.toContain(
      'Connecting PowerSync to a MySQL replica is not supported. Please connect PowerSync directly to the primary server.'
    );
    expect(query).toHaveBeenCalledWith('SHOW SLAVE STATUS', []);
    expect(query).not.toHaveBeenCalledWith('SHOW REPLICA STATUS', []);
  });

  function createConnection(options: { version: string; replicaStatuses: Record<string, unknown>[] }) {
    return createMockMySQLConnection(async (sql) => {
      switch (sql.trim()) {
        case 'SELECT VERSION() as version':
          return [[{ version: options.version }], []];
        case 'SHOW REPLICA STATUS':
        case 'SHOW SLAVE STATUS':
          return [options.replicaStatuses, []];
        case "SHOW VARIABLES LIKE 'binlog_format';":
          return [[{ Value: 'ROW' }], []];
        case "SHOW GLOBAL VARIABLES LIKE 'binlog_row_image';":
          return [[{ Value: 'FULL' }], []];
        default:
          if (sql.includes('@@GLOBAL.gtid_mode AS gtid_mode')) {
            return [
              [
                {
                  gtid_mode: 'ON',
                  log_bin: 1,
                  server_id: 1,
                  binlog_file: '/var/lib/mysql/binlog',
                  binlog_index_file: '/var/lib/mysql/binlog.index'
                }
              ],
              []
            ];
          }
          throw new Error(`Unexpected query: ${sql}`);
      }
    });
  }
});
