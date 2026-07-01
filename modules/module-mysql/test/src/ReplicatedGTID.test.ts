import { ReplicatedGTID } from '@module/common/ReplicatedGTID.js';
import { describe, expect, test } from 'vitest';

describe('ReplicatedGTID', () => {
  const POSITION = { filename: 'binlog.000042', offset: 1234 };

  describe('comparable', () => {
    test('single UUID with a single range', () => {
      const gtid = new ReplicatedGTID({
        raw_gtid: 'a7d0ff7b-0c0e-11f0-8b38-566fbaa00004:1-17',
        position: POSITION
      });
      expect(gtid.comparable).toEqual('0000000000000017|a7d0ff7b-0c0e-11f0-8b38-566fbaa00004:1-17|binlog.000042|1234');
    });

    test('single UUID with a bare transaction id', () => {
      const gtid = new ReplicatedGTID({
        raw_gtid: 'a7d0ff7b-0c0e-11f0-8b38-566fbaa00004:5',
        position: POSITION
      });
      expect(gtid.comparable.split('|')[0]).toEqual('0000000000000005');
    });

    test('single UUID with multiple intervals uses the maximum transaction id', () => {
      const gtid = new ReplicatedGTID({
        raw_gtid: 'a7d0ff7b-0c0e-11f0-8b38-566fbaa00004:1-5:11-18',
        position: POSITION
      });
      expect(gtid.comparable.split('|')[0]).toEqual('0000000000000018');
    });

    test('multiple server UUIDs joined with a newline (gtid_executed format)', () => {
      // SHOW MASTER STATUS returns multi-UUID GTID sets joined with ',\n'
      const raw = '2e35321d-0c0e-11f0-8b38-566fbaa00004:1-17,\n314306f3-ff7b-11ef-a0e0-566fbaa00002:1-2734181';
      const gtid = new ReplicatedGTID({ raw_gtid: raw, position: POSITION });
      expect(gtid.comparable).not.toContain('NaN');
      expect(gtid.comparable.split('|')[0]).toEqual('0000000002734181');
    });

    test('multiple server UUIDs where the first UUID holds the maximum', () => {
      const gtid = new ReplicatedGTID({
        raw_gtid: 'a7d0ff7b-0c0e-11f0-8b38-566fbaa00004:1-100,b7d0ff7b-0c0e-11f0-8b38-566fbaa00004:1-3',
        position: POSITION
      });
      expect(gtid.comparable.split('|')[0]).toEqual('0000000000000100');
    });

    test('multiple server UUIDs with multi-interval members', () => {
      const gtid = new ReplicatedGTID({
        raw_gtid: 'a7d0ff7b-0c0e-11f0-8b38-566fbaa00004:1-5:20-30,\nb7d0ff7b-0c0e-11f0-8b38-566fbaa00004:1-8',
        position: POSITION
      });
      expect(gtid.comparable.split('|')[0]).toEqual('0000000000000030');
    });

    test('ZERO GTID format is stable', () => {
      expect(ReplicatedGTID.ZERO.comparable).toEqual('0000000000000000|0:0||0');
    });

    test('empty GTID set falls back to the ZERO GTID', () => {
      const empty = new ReplicatedGTID({ raw_gtid: '', position: POSITION });
      expect(empty.comparable).toEqual(ReplicatedGTID.ZERO.comparable);

      const noRanges = new ReplicatedGTID({ raw_gtid: 'a7d0ff7b-0c0e-11f0-8b38-566fbaa00004', position: POSITION });
      expect(noRanges.comparable).toEqual(ReplicatedGTID.ZERO.comparable);
    });

    test('unparseable segments are skipped and never produce NaN', () => {
      const trailingGarbage = new ReplicatedGTID({
        raw_gtid: 'a7d0ff7b-0c0e-11f0-8b38-566fbaa00004:1-17,\ngarbage-no-colon',
        position: POSITION
      });
      expect(trailingGarbage.comparable).not.toContain('NaN');
      expect(trailingGarbage.comparable.split('|')[0]).toEqual('0000000000000017');

      const garbageInterval = new ReplicatedGTID({
        raw_gtid: 'a7d0ff7b-0c0e-11f0-8b38-566fbaa00004:abc-def:1-9',
        position: POSITION
      });
      expect(garbageInterval.comparable).not.toContain('NaN');
      expect(garbageInterval.comparable.split('|')[0]).toEqual('0000000000000009');
    });
  });

  describe('serialization', () => {
    test('round-trips a multi-UUID GTID set', () => {
      const raw = '2e35321d-0c0e-11f0-8b38-566fbaa00004:1-17,\n314306f3-ff7b-11ef-a0e0-566fbaa00002:1-2734181';
      const gtid = new ReplicatedGTID({ raw_gtid: raw, position: POSITION });

      const deserialized = ReplicatedGTID.fromSerialized(gtid.comparable);
      expect(deserialized.raw).toEqual(raw);
      expect(deserialized.position).toEqual(POSITION);
      expect(deserialized.comparable).toEqual(gtid.comparable);
    });

    test('throws on malformed serialized GTIDs', () => {
      expect(() => ReplicatedGTID.fromSerialized('abc')).toThrow();
      // Missing binlog offset
      expect(() => ReplicatedGTID.fromSerialized('0000000000000001|uuid:1|binlog.000001')).toThrow();
      expect(() => ReplicatedGTID.fromSerialized('0000000000000001|uuid:1|binlog.000001|notanumber')).toThrow();
    });
  });

  describe('LSN ordering', () => {
    test('LSNs for the same transaction order by binlog offset', () => {
      // Note: the binlog offset is not zero-padded, so lexicographic ordering only holds for
      // offsets with the same number of digits. This is sufficient for the checkpoint gate since
      // heartbeat keepalive LSNs are byte-identical to the last commit LSN, but is documented
      // here as a known limitation of the format (which cannot change for compatibility with
      // LSNs already persisted in bucket storage).
      const raw = 'a7d0ff7b-0c0e-11f0-8b38-566fbaa00004:18';
      const transactionStart = new ReplicatedGTID({
        raw_gtid: raw,
        position: { filename: 'binlog.000042', offset: 157 }
      });
      const transactionEnd = new ReplicatedGTID({
        raw_gtid: raw,
        position: { filename: 'binlog.000042', offset: 300 }
      });
      expect(transactionStart.comparable < transactionEnd.comparable).toBeTruthy();
    });

    test('correct LSNs order against legacy NaN-corrupted LSNs as documented', () => {
      // LSNs produced by the previous multi-UUID parsing bug contain a literal 'NaN' padded transaction id.
      const legacyPoisoned = '0000000000000NaN|2e35321d-0c0e-11f0-8b38-566fbaa00004:1-17|binlog.000042|1234';

      // Instances with transaction ids >= 1000 sort above the corrupted LSN and self-heal
      const highTransaction = new ReplicatedGTID({
        raw_gtid: '2e35321d-0c0e-11f0-8b38-566fbaa00004:1-39489900',
        position: POSITION
      });
      expect(highTransaction.comparable > legacyPoisoned).toBeTruthy();

      // Instances with transaction ids < 1000 still sort below it ('N' > any digit) and require a resync
      const lowTransaction = new ReplicatedGTID({
        raw_gtid: '2e35321d-0c0e-11f0-8b38-566fbaa00004:1-999',
        position: POSITION
      });
      expect(lowTransaction.comparable < legacyPoisoned).toBeTruthy();
    });
  });
});
