import { AuthorizationError, ErrorCode } from '@powersync/lib-services-framework';
import * as jose from 'jose';
import * as urijs from 'uri-js';
import * as uuid from 'uuid';
import { KeySpec } from './KeySpec.js';
import { KeyStore } from './KeyStore.js';

export function mapJoseError(error: jose.errors.JOSEError, token: string): AuthorizationError {
  const tokenDetails = tokenDebugDetails(token);
  if (error.code === jose.errors.JWSInvalid.code || error.code === jose.errors.JWTInvalid.code) {
    return new AuthorizationError(ErrorCode.PSYNC_S2101, 'Token is not a well-formed JWT. Check the token format.', {
      tokenDetails,
      cause: error
    });
  } else if (error.code === jose.errors.JWTClaimValidationFailed.code) {
    // Jose message: missing required "sub" claim
    const claim = (error as jose.errors.JWTClaimValidationFailed).claim;
    return new AuthorizationError(
      ErrorCode.PSYNC_S2101,
      `JWT payload is missing a required claim ${JSON.stringify(claim)}`,
      {
        cause: error,
        tokenDetails
      }
    );
  } else if (error.code == jose.errors.JWTExpired.code) {
    // Jose message: "exp" claim timestamp check failed
    return new AuthorizationError(ErrorCode.PSYNC_S2103, `JWT has expired`, {
      cause: error,
      tokenDetails
    });
  }
  return new AuthorizationError(ErrorCode.PSYNC_S2101, error.message, { cause: error });
}

export function mapAuthError(error: any, token: string): AuthorizationError {
  if (error instanceof AuthorizationError) {
    error.tokenDetails ??= tokenDebugDetails(token);
    return error;
  } else if (error instanceof jose.errors.JOSEError) {
    return mapJoseError(error, token);
  }
  return new AuthorizationError(ErrorCode.PSYNC_S2101, error.message, {
    cause: error,
    tokenDetails: tokenDebugDetails(token)
  });
}

export function mapJoseConfigError(error: jose.errors.JOSEError): AuthorizationError {
  return new AuthorizationError(ErrorCode.PSYNC_S2201, error.message ?? 'Authorization error', { cause: error });
}

export function mapAuthConfigError(error: any): AuthorizationError {
  if (error instanceof AuthorizationError) {
    return error;
  } else if (error instanceof jose.errors.JOSEError) {
    return mapJoseConfigError(error);
  }
  return new AuthorizationError(ErrorCode.PSYNC_S2201, error.message ?? 'Auth configuration error', { cause: error });
}

/**
 * Decode token for debugging purposes.
 *
 * We use this to add details to our logs. We don't log the entire token, since it may for example
 * a password incorrectly used as a token.
 */
export function tokenDebugDetails(token: string): string {
  return parseTokenDebug(token).description;
}

function parseTokenDebug(token: string) {
  try {
    // For valid tokens, we return the header and payload
    const header = jose.decodeProtectedHeader(token);
    const payload = jose.decodeJwt(token);
    const isSupabase = typeof payload.iss == 'string' && payload.iss.includes('supabase.co');
    const isSharedSecret = isSupabase && header.alg === 'HS256';

    return {
      header,
      payload,
      isSupabase,
      isSharedSecret: isSharedSecret,
      description: `<header: ${JSON.stringify(header)} payload: ${JSON.stringify(payload)}>`
    };
  } catch (e) {
    // Token fails to parse. Return some details.
    return { description: invalidTokenDetails(token) };
  }
}

function invalidTokenDetails(token: string): string {
  const parts = token.split('.');
  if (parts.length !== 3) {
    return `<token with ${parts.length} parts (needs 3), length=${token.length}>`;
  }

  const [headerB64, payloadB64, signatureB64] = parts;

  try {
    JSON.parse(Buffer.from(headerB64, 'base64url').toString('utf8'));
  } catch (e) {
    return `<token with unparsable header>`;
  }

  try {
    JSON.parse(Buffer.from(payloadB64, 'base64url').toString('utf8'));
  } catch (e) {
    return `<token with unparsable payload>`;
  }
  try {
    Buffer.from(signatureB64, 'base64url');
  } catch (e) {
    return `<token with unparsable signature>`;
  }

  return `<invalid JWT, length=${token.length}>`;
}

export interface SupabaseAuthDetails {
  projectId: string;
  url: string;
  hostname: string;
}

export function getSupabaseJwksUrl(connection: any): SupabaseAuthDetails | null {
  if (connection == null) {
    return null;
  } else if (connection.type != 'postgresql') {
    return null;
  }

  let hostname: string | undefined = connection.hostname;
  if (hostname == null && typeof connection.uri == 'string') {
    hostname = urijs.parse(connection.uri).host;
  }
  if (hostname == null) {
    return null;
  }

  const match = /db.(\w+).supabase.co/.exec(hostname);
  if (match == null) {
    return null;
  }
  const projectId = match[1];

  return { projectId, hostname, url: `https://${projectId}.supabase.co/auth/v1/.well-known/jwks.json` };
}

export function debugKeyNotFound(
  keyStore: KeyStore,
  keys: KeySpec[],
  token: string
): { configurationDetails: string; tokenDetails: string } {
  const knownKeys = keys.map((key) => key.description).join(', ');
  const td = parseTokenDebug(token);
  const tokenDetails = td.description;
  const configuredSupabase = keyStore.supabaseAuthDebug;

  // Cases to check:
  // 1. Is Supabase token, but supabase auth not enabled.
  // 2. Is Supabase HS256 token, but no secret configured.
  // 3. Is Supabase singing key token, but no Supabase signing keys configured.
  // 4. Supabase project id mismatch.

  if (td.isSharedSecret) {
    // Supabase HS256 token
    // UUID: HS256 (Shared Secret)
    // Other: Legacy HS256 (Shared Secret)
    // Not a big difference between the two other than terminology used on Supabase.
    const isLegacy = uuid.validate(td.header.kid) ? false : true;
    const addMessage =
      configuredSupabase.jwksEnabled && !isLegacy
        ? ' Use asymmetric keys on Supabase (RSA or ECC) to allow automatic key retrieval.'
        : '';
    if (!configuredSupabase.sharedSecretEnabled) {
      return {
        configurationDetails: `Token is a Supabase ${isLegacy ? 'Legacy ' : ''}HS256 (Shared Secret) token, but Supabase JWT secret is not configured.${addMessage}`,
        tokenDetails
      };
    } else {
      return {
        // This is an educated guess
        configurationDetails: `Token is a Supabase ${isLegacy ? 'Legacy ' : ''}HS256 (Shared Secret) token, but configured Supabase JWT secret does not match.${addMessage}`,
        tokenDetails
      };
    }
  } else if (td.isSupabase) {
    // Supabase JWT Signing Keys
    if (!configuredSupabase.jwksEnabled) {
      if (configuredSupabase.jwksDetails != null) {
        return {
          configurationDetails: `Token uses Supabase JWT Signing Keys, but Supabase Auth is not enabled`,
          tokenDetails
        };
      } else {
        return {
          configurationDetails: `Token uses Supabase JWT Signing Keys, but no Supabase connection is configured`,
          tokenDetails
        };
      }
    } else if (configuredSupabase.jwksDetails != null) {
      const configuredProjectId = configuredSupabase.jwksDetails.projectId;
      const issuer = td.payload.iss as string; // Is a string since since isSupabase is true
      if (!issuer.includes(configuredProjectId)) {
        return {
          configurationDetails: `Supabase project id mismatch. Expected project: ${configuredProjectId}, got issuer: ${issuer}`,
          tokenDetails
        };
      } else {
        // Project id matches, but no matching keys found
        return {
          configurationDetails: `Supabase signing keys configured, but no matching keys found. Known keys: ${knownKeys}`,
          tokenDetails
        };
      }
    }
  }

  return { configurationDetails: `Known keys: ${knownKeys}`, tokenDetails: tokenDebugDetails(token) };
}
