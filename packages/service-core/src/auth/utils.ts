import { AuthorizationError2, ErrorCode } from '@powersync/lib-services-framework';
import * as jose from 'jose';

export function mapJoseError(error: jose.errors.JOSEError): AuthorizationError2 {
  // TODO: improved message for exp issues, etc
  if (error.code === 'ERR_JWS_INVALID' || error.code === 'ERR_JWT_INVALID') {
    throw new AuthorizationError2(ErrorCode.PSYNC_S2101, 'Token is not a well-formed JWT. Check the token format.', {
      details: error.message
    });
  }
  return new AuthorizationError2(ErrorCode.PSYNC_S2101, error.message, { cause: error });
}

export function mapAuthError(error: any): AuthorizationError2 {
  if (error instanceof AuthorizationError2) {
    return error;
  } else if (error instanceof jose.errors.JOSEError) {
    return mapJoseError(error);
  }
  return new AuthorizationError2(ErrorCode.PSYNC_S2101, error.message, { cause: error });
}

export function mapJoseConfigError(error: jose.errors.JOSEError): AuthorizationError2 {
  return new AuthorizationError2(ErrorCode.PSYNC_S2201, error.message, { cause: error });
}

export function mapAuthConfigError(error: any): AuthorizationError2 {
  if (error instanceof AuthorizationError2) {
    return error;
  } else if (error instanceof jose.errors.JOSEError) {
    return mapJoseConfigError(error);
  }
  return new AuthorizationError2(ErrorCode.PSYNC_S2201, error.message, { cause: error });
}
