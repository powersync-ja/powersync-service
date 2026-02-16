import { BaseJwtPayload } from '@powersync/service-sync-rules';

/**
 * Payload from a JWT, always signed.
 */
export class JwtPayload extends BaseJwtPayload {
  /**
   * Stringified version of sub. Used where we need a string user identifier, such as in write checkpoints
   * and per-user metrics.
   */
  public readonly userIdString: string;

  constructor(parsedPayload: Record<string, any>) {
    super(parsedPayload);

    this.userIdString = this.userIdJson?.toString() ?? 'null';
  }

  get exp(): number {
    // Verified to be a number when parsing the token.
    return this.parsedPayload.exp;
  }
}
