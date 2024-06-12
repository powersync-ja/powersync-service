import * as jose from 'jose';
import secs from '../util/secs.js';
import { KeyOptions, KeySpec, SUPPORTED_ALGORITHMS } from './KeySpec.js';
import { KeyCollector } from './KeyCollector.js';
import { JwtPayload } from './JwtPayload.js';
import { logger } from '../system/Logger.js';

/**
 * KeyStore to get keys and verify tokens.
 *
 *
 * Similar to micro_auth's KeyStore, but with different caching and error handling.
 *
 * We generally assume that:
 * 1. If we have a key kid matching a JWT kid, that is the correct key.
 *    We don't look for other keys, even if there are algorithm or other issues.
 * 2. Otherwise, iterate through "wildcard" keys and look for a matching signature.
 *    Wildcard keys are any key defined without a kid.
 *
 * # Security considerations
 *
 * Some places for security holes:
 * 1. Signature verification not done correctly: We rely on jose.jwtVerify() to do this correctly.
 * 2. Using a key that has been revoked - see CachedKeyCollector's refresh strategy.
 * 3. Using a key for the wrong purpose (e.g. key.use != 'sig'). Checked in RemoteJWKSCollector.
 * 4. Not checking all attributes, e.g. a JWT trusted by the global firebase key, but has the wrong aud. Correct aud must be configured.
 * 5. Using the incorrect algorithm, e.g. 'none', or using public key as a shared key.
 *    We check the algorithm for each JWT against the matching key's configured algorithm or algorithm family.
 *
 * # Errors
 *
 * If we have a matching kid, we can generally get a detailed error (e.g. signature verification failed, invalid algorithm, etc).
 * If we don't have a matching kid, we'll generally just get an error "Could not find an appropriate key...".
 */
export class KeyStore {
  private collector: KeyCollector;

  constructor(collector: KeyCollector) {
    this.collector = collector;
  }

  async verifyJwt(token: string, options: { defaultAudiences: string[]; maxAge: string }): Promise<JwtPayload> {
    const { result, keyOptions } = await this.verifyInternal(token, {
      // audience is not checked here, since we vary the allowed audience based on the key
      // audience: options.defaultAudiences,
      clockTolerance: 60,
      // More specific algorithm checking is done when selecting the key to use.
      algorithms: SUPPORTED_ALGORITHMS,
      requiredClaims: ['aud', 'sub', 'iat', 'exp']
    });

    let audiences = options.defaultAudiences;
    if (keyOptions.requiresAudience) {
      // Replace the audience, don't add
      audiences = keyOptions.requiresAudience;
    }

    const tokenPayload = result.payload;

    let aud = tokenPayload.aud!;
    if (!Array.isArray(aud)) {
      aud = [aud];
    }
    if (
      !aud.some((a) => {
        return audiences.includes(a);
      })
    ) {
      throw new jose.errors.JWTClaimValidationFailed('unexpected "aud" claim value', 'aud', 'check_failed');
    }

    const tokenDuration = tokenPayload.exp! - tokenPayload.iat!;

    // Implement our own maxAge validation, that rejects the token immediately if expiration
    // is too far into the future.
    const maxAge = keyOptions.maxLifetimeSeconds ?? secs(options.maxAge);
    if (tokenDuration > maxAge) {
      throw new jose.errors.JWTInvalid(`Token must expire in a maximum of ${maxAge} seconds, got ${tokenDuration}`);
    }

    const parameters = tokenPayload.parameters;
    if (parameters != null && (Array.isArray(parameters) || typeof parameters != 'object')) {
      throw new jose.errors.JWTInvalid('parameters must be an object');
    }

    return {
      ...(tokenPayload as any),
      parameters: {
        user_id: tokenPayload.sub,
        ...parameters
      }
    };
  }

  private async verifyInternal(token: string, options: jose.JWTVerifyOptions) {
    let keyOptions: KeyOptions | undefined = undefined;
    const result = await jose.jwtVerify(
      token,
      async (header) => {
        let key = await this.getCachedKey(token, header);
        keyOptions = key.options;
        return key.key;
      },
      options
    );
    return { result, keyOptions: keyOptions! };
  }

  private async getCachedKey(token: string, header: jose.JWTHeaderParameters): Promise<KeySpec> {
    const kid = header.kid;
    const { keys, errors } = await this.collector.getKeys();
    if (kid) {
      // key has kid: JWK with exact kid, or JWK without kid
      // key without kid: JWK without kid only
      for (let key of keys) {
        if (key.kid == kid) {
          if (!key.matchesAlgorithm(header.alg)) {
            throw new jose.errors.JOSEAlgNotAllowed(`Unexpected token algorithm ${header.alg}`);
          }
          return key;
        }
      }
    }

    for (let key of keys) {
      // Checks signature and algorithm
      if (key.kid != null) {
        // Not a wildcard key
        continue;
      }
      if (!key.matchesAlgorithm(header.alg)) {
        continue;
      }

      if (await key.isValidSignature(token)) {
        return key;
      }
    }

    if (errors.length > 0) {
      throw errors[0];
    } else {
      // No key found
      // Trigger refresh of the keys - might be ready by the next request.
      this.collector.noKeyFound?.().catch((e) => {
        // Typically this error would be stored on the collector.
        // This is just a last resort error handling.
        logger.error(`Failed to refresh keys`, e);
      });

      throw new jose.errors.JOSEError(
        'Could not find an appropriate key in the keystore. The key is missing or no key matched the token KID'
      );
    }
  }
}
