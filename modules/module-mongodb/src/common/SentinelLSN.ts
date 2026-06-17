import { mongo } from '@powersync/lib-service-mongodb';
import { storage } from '@powersync/service-core';

export type SentinelLSNSpecification = {
  sentinel: bigint;
  /**
   * Resume tokens are opaque on sentinel-based sources (e.g. Cosmos DB). The
   * sentinel component is the comparable position; this token is only used to
   * resume the change stream.
   */
  resume_token?: mongo.ResumeToken | null;
};

const DELIMINATOR = '|';

/**
 * Width of the sentinel coordinate, matching a MongoDB cluster timestamp
 * (64-bit → 16 hex chars). The sentinel counter is a 64-bit `Long`, so this is
 * sufficient and never truncates.
 */
const SENTINEL_HEX_LENGTH = 16;

/**
 * LSN for sentinel-based checkpointing (sources without a usable clusterTime,
 * e.g. Cosmos DB). The ordered coordinate is a monotonic sentinel counter; the
 * opaque resume token is carried alongside it only for `resumeAfter`.
 *
 * The coordinate is serialized as a 16-hex-char value with the **same shape as
 * a MongoDB timestamp LSN** ({@link MongoLSN}): the high 32 bits resemble epoch
 * seconds and the low 32 bits an increment. This is deliberate — it makes
 * sentinel LSNs directly string-comparable with timestamp LSNs, so a sentinel
 * coordinate (seeded at the current epoch seconds; see createCosmosCheckpointLsn)
 * always sorts **above** any real-timestamp LSN issued in the past.
 *
 * It only *resembles* a timestamp. The value is a synthetic monotonic counter,
 * not a real cluster time, and must never be used as one (e.g. it is never fed
 * to `startAtOperationTime`; the sentinel implementation resumes purely via the
 * resume token).
 */
export class SentinelLSN {
  static ZERO = new SentinelLSN({ sentinel: 0n });

  static fromSerialized(comparable: string): SentinelLSN {
    const [sentinelString, resumeString] = comparable.split(DELIMINATOR);

    return new SentinelLSN({
      sentinel: BigInt(`0x${sentinelString}`),
      resume_token: resumeString ? storage.deserializeBson(Buffer.from(resumeString, 'base64')).resumeToken : null
    });
  }

  constructor(protected options: SentinelLSNSpecification) {}

  get sentinel() {
    return this.options.sentinel;
  }

  get resumeToken() {
    return this.options.resume_token;
  }

  get comparable() {
    // padStart(16) of the combined 64-bit value yields the identical string to
    // MongoLSN's high(8)+low(8) formatting, keeping the two formats comparable.
    const sentinel = this.sentinel.toString(16).padStart(SENTINEL_HEX_LENGTH, '0');
    const segments = [sentinel];

    if (this.resumeToken) {
      segments.push(storage.serializeBson({ resumeToken: this.resumeToken }).toString('base64'));
    }

    return segments.join(DELIMINATOR);
  }

  toString() {
    return this.comparable;
  }
}
