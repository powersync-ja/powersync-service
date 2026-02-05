import { AuthorizationError, AuthorizationResponse, ErrorCode } from '@powersync/lib-services-framework';
import * as auth from '../auth/auth-index.js';
import { ServiceContext } from '../system/ServiceContext.js';
import { BasicRouterRequest, Context, RequestEndpointHandlerPayload } from './router.js';

export function endpoint(req: BasicRouterRequest) {
  const protocol = req.headers['x-forwarded-proto'] ?? req.protocol;
  const host = req.hostname;
  return `${protocol}://${host}`;
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
      error: new AuthorizationError(ErrorCode.PSYNC_S2106, 'Authentication required')
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

  try {
    const maxAge = configuration.token_max_expiration;
    const parsedToken = await configuration.client_keystore.verifyJwt(token, {
      defaultAudiences: configuration.jwt_audiences,
      maxAge: maxAge
    });
    const safeToken = {
      ...parsedToken,
      // Ensure the sub is a string as it can be a number
      sub: parsedToken.sub.toString()
    };

    return {
      context: {
        // Ensure the JWT sub is a string
        user_id: safeToken.sub,
        token_payload: safeToken
      }
    };
  } catch (err) {
    return {
      context: null,
      tokenError: auth.mapAuthError(err, token)
    };
  }
}

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
