import { MongoLSN, ZERO_LSN } from '@module/common/MongoLSN.js';
import { mongo } from '@powersync/lib-service-mongodb';
import { describe, expect, test } from 'vitest';

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
    ).true;

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
    ).true;

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
    ).true;

    // Resume token should not be required for comparison
    expect(
      new MongoLSN({
        timestamp: mongo.Timestamp.fromNumber(10),
        resume_token: { _data: '2' }
      }).comparable > // Switching the order to test this case
        new MongoLSN({
          timestamp: mongo.Timestamp.fromNumber(9)
        }).comparable
    ).true;

    // Comparison should be backwards compatible with old LSNs
    expect(
      new MongoLSN({
        timestamp: mongo.Timestamp.fromNumber(10),
        resume_token: { _data: '2' }
      }).comparable > ZERO_LSN
    ).true;
    expect(
      new MongoLSN({
        timestamp: mongo.Timestamp.fromNumber(10),
        resume_token: { _data: '2' }
      }).comparable >
        new MongoLSN({
          timestamp: mongo.Timestamp.fromNumber(1)
        }).comparable.split('|')[0] // Simulate an old LSN
    ).true;
    expect(
      new MongoLSN({
        timestamp: mongo.Timestamp.fromNumber(1),
        resume_token: { _data: '2' }
      }).comparable <
        new MongoLSN({
          timestamp: mongo.Timestamp.fromNumber(10)
        }).comparable.split('|')[0] // Simulate an old LSN
    ).true;
  });
});
