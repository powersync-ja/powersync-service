import { AuthorizationError, ErrorCode } from '@powersync/lib-services-framework';
import * as jose from 'jose';

export function mapJoseError(error: jose.errors.JOSEError): AuthorizationError {
  // TODO: improved message for exp issues, etc
  if (error.code === jose.errors.JWSInvalid.code || error.code === jose.errors.JWTInvalid.code) {
    throw new AuthorizationError(ErrorCode.PSYNC_S2101, 'Token is not a well-formed JWT. Check the token format.', {
      details: error.message
    });
  } else if (error.code === jose.errors.JWTClaimValidationFailed.code) {
    // Example: missing required "sub" claim
    const claim = (error as jose.errors.JWTClaimValidationFailed).claim;
    throw new AuthorizationError(
      ErrorCode.PSYNC_S2101,
      `JWT payload is missing a required claim ${JSON.stringify(claim)}`,
      {
        cause: error
      }
    );
  }
  return new AuthorizationError(ErrorCode.PSYNC_S2101, error.message, { cause: error });
}

export function mapAuthError(error: any): AuthorizationError {
  if (error instanceof AuthorizationError) {
    return error;
  } else if (error instanceof jose.errors.JOSEError) {
    return mapJoseError(error);
  }
  return new AuthorizationError(ErrorCode.PSYNC_S2101, error.message, { cause: error });
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
