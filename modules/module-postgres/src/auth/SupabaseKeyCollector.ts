import * as lib_postgres from '@powersync/lib-service-postgres';
import { auth, KeyResult } from '@powersync/service-core';
import * as pgwire from '@powersync/service-jpgwire';
import * as jose from 'jose';

import * as types from '../types/types.js';
import { AuthorizationError, ErrorCode } from '@powersync/lib-services-framework';

/**
 * Fetches key from the Supabase database.
 *
 * Unfortunately, despite the JWTs containing a kid, we have no way to lookup that kid
 * before receiving a valid token.
 *
 * @deprecated Supabase is removing support for "app.settings.jwt_secret". This is likely to not function anymore, except in some self-hosted setups.
 */
export class SupabaseKeyCollector implements auth.KeyCollector {
  private pool: pgwire.PgClient;

  private keyOptions: auth.KeyOptions = {
    requiresAudience: ['authenticated'],
    maxLifetimeSeconds: 86400 * 7 + 1200 // 1 week + 20 minutes margin
  };

  constructor(connectionConfig: types.ResolvedConnectionConfig) {
    this.pool = pgwire.connectPgWirePool(connectionConfig, {
      // To avoid overloading the source database with open connections,
      // limit to a single connection, and close the connection shortly
      // after using it.
      idleTimeout: 5_000,
      maxSize: 1
    });
  }

  shutdown() {
    return this.pool.end();
  }

  async getKeys(): Promise<KeyResult> {
    let row: { jwt_secret: string };
    try {
      const rows = pgwire.pgwireRows(
        await lib_postgres.retriedQuery(this.pool, `SELECT current_setting('app.settings.jwt_secret') as jwt_secret`)
      );
      row = rows[0] as any;
    } catch (e) {
      if (e.message?.includes('unrecognized configuration parameter')) {
        throw new AuthorizationError(
          ErrorCode.PSYNC_S2201,
          'No JWT secret found in Supabase database. Manually configure the secret.'
        );
      } else {
        throw e;
      }
    }
    const secret = row?.jwt_secret as string | undefined;
    if (secret == null) {
      return {
        keys: [],
        errors: [
          new AuthorizationError(
            ErrorCode.PSYNC_S2201,
            'No JWT secret found in Supabase database. Manually configure the secret.'
          )
        ]
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
