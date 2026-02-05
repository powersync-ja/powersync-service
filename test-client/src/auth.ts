import * as jose from 'jose';
import * as fs from 'node:fs/promises';
import * as yaml from 'yaml';

export interface CredentialsOptions {
  token?: string;
  endpoint?: string;
  config?: string;
  sub?: string;
  aud?: string[];
  exp?: string;
}

export interface Credentials {
  endpoint: string;
  token: string;
}

export async function getCredentials(options: CredentialsOptions): Promise<Credentials> {
  if (options.token != null) {
    if (options.endpoint != null) {
      return { token: options.token, endpoint: options.endpoint };
    }

    const parsed = jose.decodeJwt(options.token);
    const aud = Array.isArray(parsed.aud) ? parsed.aud[0] : parsed.aud;
    if (!(aud ?? '').startsWith('http')) {
      throw new Error(`Specify endpoint, or aud in the token`);
    }

    return { token: options.token, endpoint: aud! };
  }

  if (options.config == null) {
    throw new Error(`Specify token or config path`);
  }

  const file = await fs.readFile(options.config, 'utf-8');
  const parsed = yaml.parse(file);

  const keys = (parsed.client_auth?.jwks?.keys ?? []).filter(
    (key: any) => key.alg === 'HS256' || key.alg === 'RS256'
  );

  if (keys.length === 0) {
    throw new Error('No HS256 or RS256 key found in the config');
  }

  const rawKey = keys[0];

  if (rawKey.alg === 'RS256' && rawKey.d == null) {
    throw new Error('RS256 key must include private key parameter "d"');
  }

  let endpoint = options.endpoint;
  if (endpoint == null) {
    endpoint = `http://127.0.0.1:${parsed.port ?? 8080}`;
  }

  const aud = [
    ...(parsed.client_auth?.audience ?? []),
    ...(options.aud ?? []),
    endpoint
  ].filter(Boolean);

  const key = await jose.importJWK(rawKey, rawKey.alg);

  const sub = options.sub ?? 'test_user';
  const exp = options.exp ?? '24h';

  const token = await new jose.SignJWT({})
    .setProtectedHeader({
      alg: rawKey.alg,
      kid: rawKey.kid
    })
    .setSubject(sub)
    .setIssuedAt()
    .setIssuer('test-client')
    .setAudience(aud)
    .setExpirationTime(exp)
    .sign(key);

  return { token, endpoint };
}