import { MongoLSN, parseResumeTokenTimestamp, ZERO_LSN } from '@module/common/MongoLSN.js';
import { mongo } from '@powersync/lib-service-mongodb';
import { describe, expect, it, test } from 'vitest';

describe('parseResumeTokenTimestamp', () => {
  it('parses a valid resume token (1)', () => {
    const timestamp = parseResumeTokenTimestamp({ _data: '826811D298000000012B0429296E1404' });
    expect(timestamp.t).toEqual(1745998488);
    expect(timestamp.i).toEqual(1);
  });

  it('parses a valid resume token (2)', () => {
    const timestamp = parseResumeTokenTimestamp({
      _data:
        '8267B4B1F8000000322B042C0100296E5A10041831DD5EEE2B4D6495A610E5430872B6463C6F7065726174696F6E54797065003C7570646174650046646F63756D656E744B657900463C5F6964003C636865636B706F696E7400000004'
    });
    expect(timestamp.t).toEqual(1739895288);
    expect(timestamp.i).toEqual(50);
  });

  it('parses a valid resume token (3)', () => {
    const timestamp = parseResumeTokenTimestamp({
      _data:
        '826811D228000000022B042C0100296E5A10048725A7954ED247538A4851BAB78B0560463C6F7065726174696F6E54797065003C7570646174650046646F63756D656E744B657900463C5F6964003C636865636B706F696E7400000004'
    });
    expect(timestamp.t).toEqual(1745998376);
    expect(timestamp.i).toEqual(2);
  });

  it('throws for invalid prefix', () => {
    const hex = 'FF0102030405060708';
    const resumeToken: any = { _data: hex };

    expect(() => parseResumeTokenTimestamp(resumeToken)).toThrowError(/^Invalid resume token/);
  });
});

describe('mongo lsn', () => {
  test('LSN with resume tokens should be comparable', () => {
    // Values without a resume token should be comparable
    expect(
      new MongoLSN({
        timestamp: mongo.Timestamp.fromNumber(1)
      }).comparable <
        new MongoLSN({
          timestamp: mongo.Timestamp.fromNumber(10)
        }).comparable
    ).toBe(true);

    // Values with resume tokens should correctly compare
    expect(
      new MongoLSN({
        timestamp: mongo.Timestamp.fromNumber(1),
        resume_token: { _data: 'resume1' }
      }).comparable <
        new MongoLSN({
          timestamp: mongo.Timestamp.fromNumber(10),
          resume_token: { _data: 'resume2' }
        }).comparable
    ).toBe(true);

    // The resume token should not affect comparison
    expect(
      new MongoLSN({
        timestamp: mongo.Timestamp.fromNumber(1),
        resume_token: { _data: '2' }
      }).comparable <
        new MongoLSN({
          timestamp: mongo.Timestamp.fromNumber(10),
          resume_token: { _data: '1' }
        }).comparable
    ).toBe(true);

    // Resume token should not be required for comparison
    expect(
      new MongoLSN({
        timestamp: mongo.Timestamp.fromNumber(10),
        resume_token: { _data: '2' }
      }).comparable > // Switching the order to test this case
        new MongoLSN({
          timestamp: mongo.Timestamp.fromNumber(9)
        }).comparable
    ).toBe(true);

    // Comparison should be backwards compatible with old LSNs
    expect(
      new MongoLSN({
        timestamp: mongo.Timestamp.fromNumber(10),
        resume_token: { _data: '2' }
      }).comparable > ZERO_LSN
    ).toBe(true);
    expect(
      new MongoLSN({
        timestamp: mongo.Timestamp.fromNumber(10),
        resume_token: { _data: '2' }
      }).comparable >
        new MongoLSN({
          timestamp: mongo.Timestamp.fromNumber(1)
        }).comparable.split('|')[0] // Simulate an old LSN
    ).toBe(true);
    expect(
      new MongoLSN({
        timestamp: mongo.Timestamp.fromNumber(1),
        resume_token: { _data: '2' }
      }).comparable <
        new MongoLSN({
          timestamp: mongo.Timestamp.fromNumber(10)
        }).comparable.split('|')[0] // Simulate an old LSN
    ).toBe(true);
  });
});
