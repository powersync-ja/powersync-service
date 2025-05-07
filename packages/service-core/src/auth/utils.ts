import { AuthorizationError, ErrorCode } from '@powersync/lib-services-framework';
import * as jose from 'jose';

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
function tokenDebugDetails(token: string): string {
  try {
    // For valid tokens, we return the header and payload
    const header = jose.decodeProtectedHeader(token);
    const payload = jose.decodeJwt(token);
    return `<header: ${JSON.stringify(header)} payload: ${JSON.stringify(payload)}>`;
  } catch (e) {
    // Token fails to parse. Return some details.
    return invalidTokenDetails(token);
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
