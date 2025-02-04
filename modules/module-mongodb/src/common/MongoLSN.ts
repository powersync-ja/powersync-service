import { mongo } from '@powersync/lib-service-mongodb';

export type MongoLSNSpecification = {
  timestamp: mongo.Timestamp;
  resume_token?: mongo.ResumeToken;
};

export const ZERO_LSN = '0000000000000000';

export class MongoLSN {
  static fromSerialized(comparable: string): MongoLSN {
    return new MongoLSN(MongoLSN.deserialize(comparable));
  }

  private static deserialize(comparable: string): MongoLSNSpecification {
    const [timestampString, resumeString] = comparable.split('|');

    const a = parseInt(timestampString.substring(0, 8), 16);
    const b = parseInt(timestampString.substring(8, 16), 16);

    return {
      timestamp: mongo.Timestamp.fromBits(b, a),
      resume_token: resumeString ? JSON.parse(resumeString) : null
    };
  }

  static ZERO = MongoLSN.fromSerialized(ZERO_LSN);

  constructor(protected options: MongoLSNSpecification) {}

  get timestamp() {
    return this.options.timestamp;
  }

  get resumeToken() {
    return this.options.resume_token;
  }

  get comparable() {
    const { timestamp, resumeToken } = this;

    const a = timestamp.high.toString(16).padStart(8, '0');
    const b = timestamp.low.toString(16).padStart(8, '0');

    return `${a}${b}|${resumeToken ? JSON.stringify(resumeToken) : ''}`;
  }

  toString() {
    return this.comparable;
  }
}
