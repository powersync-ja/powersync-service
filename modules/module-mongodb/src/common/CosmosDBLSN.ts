import { mongo } from '@powersync/lib-service-mongodb';
import { storage } from '@powersync/service-core';

export type CosmosDBLSNSpecification = {
  sentinel: bigint | number | string;
  /**
   * Cosmos DB resume tokens are opaque. The sentinel component is the
   * comparable position; this token is only used to resume the change stream.
   */
  resume_token?: mongo.ResumeToken | null;
};

const DELIMINATOR = '|';
const PREFIX = 'c';
const SENTINEL_HEX_LENGTH = 32;

export class CosmosDBLSN {
  static ZERO = new CosmosDBLSN({ sentinel: 0n });

  static fromSerialized(comparable: string): CosmosDBLSN {
    const [sentinelString, resumeString] = comparable.split(DELIMINATOR);
    if (!sentinelString.startsWith(PREFIX)) {
      throw new Error(`Invalid Cosmos DB LSN: ${comparable}`);
    }

    return new CosmosDBLSN({
      sentinel: BigInt(`0x${sentinelString.slice(PREFIX.length)}`),
      resume_token: resumeString ? storage.deserializeBson(Buffer.from(resumeString, 'base64')).resumeToken : null
    });
  }

  constructor(protected options: CosmosDBLSNSpecification) {}

  get sentinel() {
    return normalizeSentinel(this.options.sentinel);
  }

  get resumeToken() {
    return this.options.resume_token;
  }

  get comparable() {
    const sentinel = this.sentinel.toString(16).padStart(SENTINEL_HEX_LENGTH, '0');
    const segments = [`${PREFIX}${sentinel}`];

    if (this.resumeToken) {
      segments.push(storage.serializeBson({ resumeToken: this.resumeToken }).toString('base64'));
    }

    return segments.join(DELIMINATOR);
  }

  toString() {
    return this.comparable;
  }
}

export function normalizeSentinel(value: bigint | number | string | mongo.Long): bigint {
  if (typeof value == 'bigint') {
    return value;
  } else if (typeof value == 'number') {
    return BigInt(value);
  } else if (typeof value == 'string') {
    return BigInt(value);
  } else {
    return value.toBigInt();
  }
}
