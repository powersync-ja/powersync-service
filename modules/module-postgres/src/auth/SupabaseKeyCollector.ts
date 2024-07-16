import * as jose from 'jose';
import * as pgwire from '@powersync/service-jpgwire';
import { auth } from '@powersync/service-core';

import { ResolvedConnectionConfig } from '../types/types.js';
import * as pgwire_utils from '../utils/pgwire_utils.js';

/**
 * Fetches key from the Supabase database.
 *
 * Unfortunately, despite the JWTs containing a kid, we have no way to lookup that kid
 * before receiving a valid token.
 */
export class SupabaseKeyCollector implements auth.KeyCollector {
  private pool: pgwire.PgClient;

  private keyOptions: auth.KeyOptions = {
    requiresAudience: ['authenticated'],
    maxLifetimeSeconds: 86400 * 7 + 1200 // 1 week + 20 minutes margin
  };

  constructor(connectionConfig: ResolvedConnectionConfig) {
    this.pool = pgwire.connectPgWirePool(connectionConfig, {
      // To avoid overloading the source database with open connections,
      // limit to a single connection, and close the connection shortly
      // after using it.
      idleTimeout: 5_000,
      maxSize: 1
    });
  }

  async getKeys() {
    let row: { jwt_secret: string };
    try {
      const rows = pgwire.pgwireRows(
        await pgwire_utils.retriedQuery(this.pool, `SELECT current_setting('app.settings.jwt_secret') as jwt_secret`)
      );
      row = rows[0] as any;
    } catch (e) {
      if (e.message?.includes('unrecognized configuration parameter')) {
        throw new jose.errors.JOSEError(`Generate a new JWT secret on Supabase. Cause: ${e.message}`);
      } else {
        throw e;
      }
    }
    const secret = row?.jwt_secret as string | undefined;
    if (secret == null) {
      return {
        keys: [],
        errors: [new jose.errors.JWKSNoMatchingKey()]
      };
    } else {
      const key: jose.JWK = {
        kty: 'oct',
        alg: 'HS256',
        // While the secret is valid base64, the base64-encoded form is the secret value.
        k: Buffer.from(secret, 'utf8').toString('base64url')
      };
      const imported = await auth.KeySpec.importKey(key, this.keyOptions);
      return {
        keys: [imported],
        errors: []
      };
    }
  }
}
