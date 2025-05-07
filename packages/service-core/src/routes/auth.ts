import * as jose from 'jose';

import * as auth from '../auth/auth-index.js';
import { ServiceContext } from '../system/ServiceContext.js';
import * as util from '../util/util-index.js';
import { BasicRouterRequest, Context, RequestEndpointHandlerPayload } from './router.js';
import { AuthorizationError2, AuthorizationResponse, ErrorCode, ServiceError } from '@powersync/lib-services-framework';

export function endpoint(req: BasicRouterRequest) {
  const protocol = req.headers['x-forwarded-proto'] ?? req.protocol;
  const host = req.hostname;
  return `${protocol}://${host}`;
}

function devAudience(req: BasicRouterRequest): string {
  return `${endpoint(req)}/dev`;
}

/**
 * @deprecated
 *
 * Will be replaced by temporary tokens issued by PowerSync Management service.
 */
export async function issueDevToken(req: BasicRouterRequest, user_id: string, config: util.ResolvedPowerSyncConfig) {
  const iss = devAudience(req);
  const aud = devAudience(req);

  const key = config.dev.dev_key;
  if (key == null) {
    throw new Error('Auth disabled');
  }

  return await new jose.SignJWT({})
    .setProtectedHeader({ alg: key.source.alg!, kid: key.kid })
    .setSubject(user_id)
    .setIssuedAt()
    .setIssuer(iss)
    .setAudience(aud)
    .setExpirationTime('30d')
    .sign(key.key);
}

/** @deprecated */
export async function issueLegacyDevToken(
  req: BasicRouterRequest,
  user_id: string,
  config: util.ResolvedPowerSyncConfig
) {
  const iss = devAudience(req);
  const aud = config.jwt_audiences[0];

  const key = config.dev.dev_key;
  if (key == null || aud == null) {
    throw new Error('Auth disabled');
  }

  return await new jose.SignJWT({})
    .setProtectedHeader({ alg: key.source.alg!, kid: key.kid })
    .setSubject(user_id)
    .setIssuedAt()
    .setIssuer(iss)
    .setAudience(aud)
    .setExpirationTime('60m')
    .sign(key.key);
}

export async function issuePowerSyncToken(
  req: BasicRouterRequest,
  user_id: string,
  config: util.ResolvedPowerSyncConfig
) {
  const iss = devAudience(req);
  const aud = config.jwt_audiences[0];
  const key = config.dev.dev_key;
  if (key == null || aud == null) {
    throw new Error('Auth disabled');
  }

  const jwt = await new jose.SignJWT({})
    .setProtectedHeader({ alg: key.source.alg!, kid: key.kid })
    .setSubject(user_id)
    .setIssuedAt()
    .setIssuer(iss)
    .setAudience(aud)
    .setExpirationTime('5m')
    .sign(key.key);
  return jwt;
}

export function getTokenFromHeader(authHeader: string = ''): string | null {
  const tokenMatch = /^(Token|Bearer) (\S+)$/.exec(authHeader);
  if (!tokenMatch) {
    return null;
  }
  const token = tokenMatch[2];
  return token ?? null;
}

export const authUser = async (payload: RequestEndpointHandlerPayload): Promise<AuthorizationResponse> => {
  return authorizeUser(payload.context, payload.request.headers.authorization as string);
};

export async function authorizeUser(context: Context, authHeader: string = ''): Promise<AuthorizationResponse> {
  const token = getTokenFromHeader(authHeader);
  if (token == null) {
    return {
      authorized: false,
      error: new AuthorizationError2(ErrorCode.PSYNC_S2106, 'Authentication required')
    };
  }

  const { context: tokenContext, tokenError } = await generateContext(context.service_context, token);

  if (!tokenContext) {
    return {
      authorized: false,
      error: tokenError
    };
  }

  Object.assign(context, tokenContext);
  return { authorized: true };
}

export async function generateContext(serviceContext: ServiceContext, token: string) {
  const { configuration } = serviceContext;

  let tokenPayload: auth.JwtPayload;
  try {
    const maxAge = configuration.token_max_expiration;
    tokenPayload = await configuration.client_keystore.verifyJwt(token, {
      defaultAudiences: configuration.jwt_audiences,
      maxAge: maxAge
    });
    return {
      context: {
        user_id: tokenPayload.sub,
        token_payload: tokenPayload
      }
    };
  } catch (err) {
    return {
      context: null,
      tokenError: auth.mapAuthError(err)
    };
  }
}

/**
 * @deprecated
 */
export const authDevUser = async (payload: RequestEndpointHandlerPayload) => {
  const {
    context: {
      service_context: { configuration }
    }
  } = payload;

  const token = getTokenFromHeader(payload.request.headers.authorization as string);
  if (!configuration.dev.demo_auth) {
    return {
      authorized: false,
      errors: ['Authentication disabled']
    };
  }
  if (token == null) {
    return {
      authorized: false,
      errors: ['Authentication required']
    };
  }

  // Different from the configured audience.
  // Should also not be changed by keys
  const audience = [devAudience(payload.request)];

  let tokenPayload: auth.JwtPayload;
  try {
    tokenPayload = await configuration.dev_client_keystore.verifyJwt(token, {
      defaultAudiences: audience,
      maxAge: '31d'
    });
  } catch (err) {
    return {
      authorized: false,
      errors: [err.message]
    };
  }

  payload.context.user_id = tokenPayload.sub;
  return { authorized: true };
};

export const authApi = (payload: RequestEndpointHandlerPayload) => {
  const {
    context: {
      service_context: { configuration }
    }
  } = payload;
  const api_keys = configuration.api_tokens;
  if (api_keys.length == 0) {
    return {
      authorized: false,
      errors: ['Authentication disabled']
    };
  }
  const auth = (payload.request.headers.authorization as string) ?? '';

  const tokenMatch = /^(Token|Bearer) (\S+)$/.exec(auth);
  if (!tokenMatch) {
    return {
      authorized: false,
      errors: ['Authentication required']
    };
  }
  const token = tokenMatch[2];
  if (api_keys.includes(token)) {
    return { authorized: true };
  } else {
    return {
      authorized: false,
      errors: ['Authentication failed']
    };
  }
};
