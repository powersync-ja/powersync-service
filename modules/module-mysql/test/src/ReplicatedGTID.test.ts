import { ReplicatedGTID } from '@module/common/ReplicatedGTID.js';
import * as uuid from 'uuid';
import { describe, expect, test } from 'vitest';

describe('ReplicatedGTID', () => {
  const SERVER_UUID = 'a7d0ff7b-0c0e-11f0-8b38-566fbaa00004';
  const POSITION = { filename: 'binlog.000042', offset: 1234 };

  describe('single GTID', () => {
    test('exposes its raw value, server UUID, and binlog position', () => {
      const gtid = new ReplicatedGTID({
        rawGtid: `${SERVER_UUID}:5`,
        position: POSITION
      });

      expect(gtid.raw).toEqual(`${SERVER_UUID}:5`);
      expect(gtid.serverUuid).toEqual(SERVER_UUID);
      expect(gtid.position).toEqual(POSITION);
    });

    test('formats a comparable LSN using the transaction id', () => {
      const gtid = new ReplicatedGTID({
        rawGtid: `${SERVER_UUID}:17`,
        position: POSITION
      });

      expect(gtid.comparable).toEqual(`0000000000000017|${SERVER_UUID}:17|binlog.000042|1234`);
      expect(gtid.toString()).toEqual(gtid.comparable);
    });

    test('normalizes surrounding whitespace', () => {
      const gtid = new ReplicatedGTID({
        rawGtid: ` \n\t${SERVER_UUID}:17 \r\n`,
        position: POSITION
      });

      expect(gtid.raw).toEqual(`${SERVER_UUID}:17`);
      expect(gtid.serverUuid).toEqual(SERVER_UUID);
      expect(gtid.comparable).toEqual(`0000000000000017|${SERVER_UUID}:17|binlog.000042|1234`);
    });

    test('keeps the ZERO GTID format stable', () => {
      expect(ReplicatedGTID.ZERO(SERVER_UUID).raw).toEqual(`${SERVER_UUID}:0`);
      expect(ReplicatedGTID.ZERO(SERVER_UUID).comparable).toEqual(`0000000000000000|${SERVER_UUID}:0||0`);
    });
  });

  describe('validation', () => {
    test.each([
      ['', 'missing server UUID and transaction id'],
      [SERVER_UUID, 'missing transaction id'],
      [`${SERVER_UUID}:`, 'empty transaction id'],
      [`:${17}`, 'empty server UUID'],
      [`${SERVER_UUID}:1-17`, 'transaction interval'],
      [`${SERVER_UUID}:1:17`, 'multiple transaction components'],
      [`${SERVER_UUID}:abc`, 'non-numeric transaction id'],
      [`${SERVER_UUID}:-1`, 'negative transaction id'],
      [`${SERVER_UUID}:17,another-server:9`, 'comma-separated GTID set'],
      [`${SERVER_UUID}:17,\nanother-server:9`, 'newline-separated GTID set']
    ])('rejects %s (%s)', (rawGtid) => {
      expect(() => new ReplicatedGTID({ rawGtid, position: POSITION })).toThrow();
    });
  });

  describe('serialization', () => {
    test('round-trips a single GTID', () => {
      const gtid = new ReplicatedGTID({
        rawGtid: `${SERVER_UUID}:17`,
        position: POSITION
      });

      const deserialized = ReplicatedGTID.fromSerialized(gtid.comparable);

      expect(deserialized.raw).toEqual(gtid.raw);
      expect(deserialized.serverUuid).toEqual(SERVER_UUID);
      expect(deserialized.position).toEqual(POSITION);
      expect(deserialized.comparable).toEqual(gtid.comparable);
    });

    test('throws on malformed serialized GTIDs', () => {
      expect(() => ReplicatedGTID.fromSerialized('abc')).toThrow('Invalid serialized GTID');
      expect(() => ReplicatedGTID.fromSerialized(`0000000000000001|${SERVER_UUID}:1|binlog.000001`)).toThrow(
        'Invalid serialized GTID'
      );
      expect(() => ReplicatedGTID.fromSerialized(`0000000000000001|${SERVER_UUID}:1|binlog.000001|notanumber`)).toThrow(
        'Invalid BinLog offset'
      );
    });

    test('rejects a serialized GTID set', () => {
      const serialized = `0000000000000017|${SERVER_UUID}:1-17|binlog.000042|1234`;

      expect(() => ReplicatedGTID.fromSerialized(serialized)).toThrow('Expected a single transaction id');
    });
  });

  describe('binlog events', () => {
    test('creates a single GTID from a binlog event', () => {
      const gtid = ReplicatedGTID.fromBinLogEvent({
        rawGtid: {
          serverUuid: Buffer.from(uuid.parse(SERVER_UUID)),
          transactionId: 17
        },
        position: POSITION
      });

      expect(gtid.raw).toEqual(`${SERVER_UUID}:17`);
      expect(gtid.position).toEqual(POSITION);
      expect(gtid.comparable).toEqual(`0000000000000017|${SERVER_UUID}:17|binlog.000042|1234`);
    });
  });

  describe('LSN ordering', () => {
    test('orders GTIDs from the same server by transaction id', () => {
      const earlier = new ReplicatedGTID({ rawGtid: `${SERVER_UUID}:9`, position: POSITION });
      const later = new ReplicatedGTID({ rawGtid: `${SERVER_UUID}:18`, position: POSITION });

      expect(earlier.comparable < later.comparable).toBeTruthy();
    });

    test('orders LSNs for the same transaction by binlog offset', () => {
      // The binlog offset is not zero-padded, so lexicographic ordering only holds for
      // offsets with the same number of digits. This format cannot change while existing
      // LSNs remain persisted in bucket storage.
      const rawGtid = `${SERVER_UUID}:18`;
      const transactionStart = new ReplicatedGTID({
        rawGtid,
        position: { filename: 'binlog.000042', offset: 157 }
      });
      const transactionEnd = new ReplicatedGTID({
        rawGtid,
        position: { filename: 'binlog.000042', offset: 300 }
      });

      expect(transactionStart.comparable < transactionEnd.comparable).toBeTruthy();
    });
  });
});
