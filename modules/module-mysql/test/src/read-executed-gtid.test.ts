import { ReplicatedGTID } from '@module/common/ReplicatedGTID.js';
import {
  getLatestActiveGtid,
  isGtidPositionStillAvailable,
  readExecutedGtid
} from '@module/common/read-executed-gtid.js';
import { describe, expect, test } from 'vitest';
import { createMockMySQLConnection } from './util.js';

describe('read-executed-gtid', () => {
  const ACTIVE_SERVER_UUID = 'a7d0ff7b-0c0e-11f0-8b38-566fbaa00004';
  const STALE_SERVER_UUID = '314306f3-ff7b-11ef-a0e0-566fbaa00002';

  describe('getLatestActiveGtid', () => {
    test('returns the highest transaction id for the active server', async () => {
      const gtid = await getLatestActiveGtid(
        [`${STALE_SERVER_UUID}:1-1000`, `\n${ACTIVE_SERVER_UUID}:1-5:9:12-17`],
        ACTIVE_SERVER_UUID
      );

      expect(gtid).toEqual(`${ACTIVE_SERVER_UUID}:17`);
    });

    test('supports a bare transaction id', async () => {
      const gtid = await getLatestActiveGtid([`${ACTIVE_SERVER_UUID}:42`], ACTIVE_SERVER_UUID);

      expect(gtid).toEqual(`${ACTIVE_SERVER_UUID}:42`);
    });

    test('returns the active server ZERO GTID when it is absent from the GTID sets', async () => {
      await expect(getLatestActiveGtid([`${STALE_SERVER_UUID}:1-1000`], ACTIVE_SERVER_UUID)).resolves.toEqual(
        `${ACTIVE_SERVER_UUID}:0`
      );
    });
  });

  describe('readExecutedGtid', () => {
    test('reads binary log status on MySQL 8.4 and selects the active server GTID', async () => {
      const { connection, query } = createConnection({
        version: '8.4.0',
        executedGtidSet: `${STALE_SERVER_UUID}:1-1000,\n${ACTIVE_SERVER_UUID}:1-5:11-18`
      });

      const gtid = await readExecutedGtid(connection);

      expect(gtid.raw).toEqual(`${ACTIVE_SERVER_UUID}:18`);
      expect(gtid.position).toEqual({ filename: 'binlog.000042', offset: 1234 });
      expect(query).toHaveBeenCalledWith('SHOW BINARY LOG STATUS', []);
      expect(query).not.toHaveBeenCalledWith('SHOW MASTER STATUS', []);
    });

    test('reads master status on MySQL versions before 8.4', async () => {
      const { connection, query } = createConnection({
        version: '8.0.40',
        executedGtidSet: `${ACTIVE_SERVER_UUID}:1-17`
      });

      const gtid = await readExecutedGtid(connection);

      expect(gtid.raw).toEqual(`${ACTIVE_SERVER_UUID}:17`);
      expect(query).toHaveBeenCalledWith('SHOW MASTER STATUS', []);
      expect(query).not.toHaveBeenCalledWith('SHOW BINARY LOG STATUS', []);
    });

    test('returns the active server ZERO GTID when no transactions have executed', async () => {
      const { connection } = createConnection({
        version: '8.4.0',
        executedGtidSet: ' \n\t '
      });

      const gtid = await readExecutedGtid(connection);

      expect(gtid.raw).toEqual(`${ACTIVE_SERVER_UUID}:0`);
      expect(gtid.position).toEqual({ filename: 'binlog.000042', offset: 1234 });
      expect(gtid.comparable).toEqual(`0000000000000000|${ACTIVE_SERVER_UUID}:0|binlog.000042|1234`);
    });

    test('uses the active server ZERO GTID at the current position when only historical UUIDs exist', async () => {
      const { connection } = createConnection({
        version: '8.4.0',
        executedGtidSet: `${STALE_SERVER_UUID}:1-1000`
      });

      const gtid = await readExecutedGtid(connection);

      expect(gtid.raw).toEqual(`${ACTIVE_SERVER_UUID}:0`);
      expect(gtid.position).toEqual({ filename: 'binlog.000042', offset: 1234 });
      expect(gtid.comparable).toEqual(`0000000000000000|${ACTIVE_SERVER_UUID}:0|binlog.000042|1234`);
    });
  });

  describe('isGtidPositionStillAvailable', () => {
    const RESUME_GTID = new ReplicatedGTID({
      rawGtid: `${ACTIVE_SERVER_UUID}:17`,
      position: { filename: 'binlog.000042', offset: 1234 }
    });

    test('returns true when the GTID is executed and its binlog coordinate is available', async () => {
      const { connection, query } = createResumeCheckConnection({
        isExecuted: 1,
        logFiles: [{ Log_name: 'binlog.000042', File_size: 2000 }]
      });

      await expect(isGtidPositionStillAvailable(connection, RESUME_GTID)).resolves.toBe(true);
      expect(query).toHaveBeenCalledWith('SELECT GTID_SUBSET(?, @@GLOBAL.gtid_executed) AS is_executed', [
        RESUME_GTID.raw
      ]);
    });

    test('returns false when the GTID is absent after a source rewind', async () => {
      const { connection } = createResumeCheckConnection({
        isExecuted: 0,
        logFiles: [{ Log_name: 'binlog.000042', File_size: 2000 }]
      });

      await expect(isGtidPositionStillAvailable(connection, RESUME_GTID)).resolves.toBe(false);
    });

    test('validates the synthetic ZERO GTID using only its binlog coordinate', async () => {
      const zeroGtid = new ReplicatedGTID({
        rawGtid: `${ACTIVE_SERVER_UUID}:0`,
        position: { filename: 'binlog.000042', offset: 1234 }
      });
      const { connection, query } = createResumeCheckConnection({
        isExecuted: 0,
        logFiles: [{ Log_name: 'binlog.000042', File_size: 2000 }]
      });

      await expect(isGtidPositionStillAvailable(connection, zeroGtid)).resolves.toBe(true);
      expect(query).not.toHaveBeenCalledWith('SELECT GTID_SUBSET(?, @@GLOBAL.gtid_executed) AS is_executed', [
        zeroGtid.raw
      ]);
    });

    test.each([
      [[], 'the binlog file is absent'],
      [[{ Log_name: 'binlog.000042', File_size: 1000 }], 'the stored offset is past the end of the binlog']
    ])('returns false when %s (%s)', async (logFiles) => {
      const { connection, query } = createResumeCheckConnection({ isExecuted: 1, logFiles });

      await expect(isGtidPositionStillAvailable(connection, RESUME_GTID)).resolves.toBe(false);
      expect(query).not.toHaveBeenCalledWith('SELECT GTID_SUBSET(?, @@GLOBAL.gtid_executed) AS is_executed', [
        RESUME_GTID.raw
      ]);
    });
  });

  function createConnection(options: { version: string; executedGtidSet: string }) {
    return createMockMySQLConnection(async (sql) => {
      switch (sql) {
        case 'SELECT VERSION() as version':
          return [[{ version: options.version }], []];
        case 'SHOW BINARY LOG STATUS':
        case 'SHOW MASTER STATUS':
          return [
            [
              {
                File: 'binlog.000042',
                Position: '1234',
                Executed_Gtid_Set: options.executedGtidSet
              }
            ],
            []
          ];
        case 'SELECT @@server_uuid AS server_uuid':
          return [[{ server_uuid: ACTIVE_SERVER_UUID }], []];
        default:
          throw new Error(`Unexpected query: ${sql}`);
      }
    });
  }

  function createResumeCheckConnection(options: {
    isExecuted: number;
    logFiles: { Log_name: string; File_size: number }[];
  }) {
    return createMockMySQLConnection(async (sql) => {
      switch (sql) {
        case 'SHOW BINARY LOGS;':
          return [options.logFiles, []];
        case 'SELECT GTID_SUBSET(?, @@GLOBAL.gtid_executed) AS is_executed':
          return [[{ is_executed: options.isExecuted }], []];
        default:
          throw new Error(`Unexpected query: ${sql}`);
      }
    });
  }
});
