import { AuthorizationError, ErrorCode } from '@powersync/lib-services-framework';
import * as jose from 'jose';

export function mapJoseError(error: jose.errors.JOSEError): AuthorizationError {
  // TODO: improved message for exp issues, etc
  if (error.code === 'ERR_JWS_INVALID' || error.code === 'ERR_JWT_INVALID') {
    throw new AuthorizationError(ErrorCode.PSYNC_S2101, 'Token is not a well-formed JWT. Check the token format.', {
      details: error.message
    });
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
  return new AuthorizationError(ErrorCode.PSYNC_S2201, error.message, { cause: error });
}

export function mapAuthConfigError(error: any): AuthorizationError {
  if (error instanceof AuthorizationError) {
    return error;
  } else if (error instanceof jose.errors.JOSEError) {
    return mapJoseConfigError(error);
  }
  return new AuthorizationError(ErrorCode.PSYNC_S2201, error.message, { cause: error });
}
