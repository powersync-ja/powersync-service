import * as micro from '@journeyapps-platform/micro';
import { FastifyRequest } from 'fastify';
import * as jose from 'jose';

import * as auth from '../auth/auth-index.js';
import * as util from '../util/util-index.js';
import { Context } from './router.js';
import { CorePowerSyncSystem } from '../system/CorePowerSyncSystem.js';

export function endpoint(req: FastifyRequest) {
  const protocol = req.headers['x-forwarded-proto'] ?? req.protocol;
  const host = req.hostname;
  return `${protocol}://${host}`;
}

function devAudience(req: FastifyRequest): string {
  return `${endpoint(req)}/dev`;
}

/**
 * @deprecated
 *
 * Will be replaced by temporary tokens issued by PowerSync Management service.
 */
export async function issueDevToken(req: FastifyRequest, user_id: string, config: util.ResolvedPowerSyncConfig) {
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
export async function issueLegacyDevToken(req: FastifyRequest, user_id: string, config: util.ResolvedPowerSyncConfig) {
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

export async function issuePowerSyncToken(req: FastifyRequest, user_id: string, config: util.ResolvedPowerSyncConfig) {
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

export const authUser = async (payload: micro.fastify.FastifyHandlerPayload<any, Context>) => {
  return authorizeUser(payload.context, payload.request.headers.authorization);
};

export async function authorizeUser(context: Context, authHeader: string = '') {
  const token = getTokenFromHeader(authHeader);
  if (token == null) {
    return {
      authorized: false,
      errors: ['Authentication required']
    };
  }

  const { context: tokenContext, errors } = await generateContext(context.system, token);

  if (!tokenContext) {
    return {
      authorized: false,
      errors
    };
  }

  Object.assign(context, tokenContext);
  return { authorized: true };
}

export async function generateContext(system: CorePowerSyncSystem, token: string) {
  const config = system.config;

  let tokenPayload: auth.JwtPayload;
  try {
    const maxAge = config.token_max_expiration;
    tokenPayload = await system.client_keystore.verifyJwt(token, {
      defaultAudiences: config.jwt_audiences,
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
      errors: [err.message]
    };
  }
}

/**
 * @deprecated
 */
export const authDevUser = async (payload: micro.fastify.FastifyHandlerPayload<any, Context>) => {
  const context = payload.context;
  const token = getTokenFromHeader(payload.request.headers.authorization);
  if (!context.system.config.dev.demo_auth) {
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
    tokenPayload = await context.system.dev_client_keystore.verifyJwt(token, {
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

export const authApi = (payload: micro.fastify.FastifyHandlerPayload<any, Context>) => {
  const context = payload.context;
  const api_keys = context.system.config.api_tokens;
  if (api_keys.length == 0) {
    return {
      authorized: false,
      errors: ['Authentication disabled']
    };
  }
  const auth = payload.request.headers.authorization ?? '';

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
