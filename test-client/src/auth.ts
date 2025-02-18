import * as jose from 'jose';
import * as fs from 'node:fs/promises';
import * as yaml from 'yaml';

export interface CredentialsOptions {
  token?: string;
  endpoint?: string;
  config?: string;
  sub?: string;
}

export interface Credentials {
  endpoint: string;
  token: string;
}

export async function getCredentials(options: CredentialsOptions): Promise<Credentials> {
  if (options.token != null) {
    if (options.endpoint != null) {
      return { token: options.token, endpoint: options.endpoint };
    } else {
      const parsed = jose.decodeJwt(options.token);
      const aud = Array.isArray(parsed.aud) ? parsed.aud[0] : parsed.aud;
      if (!(aud ?? '').startsWith('http')) {
        throw new Error(`Specify endpoint, or aud in the token`);
      }
      return {
        token: options.token,
        endpoint: aud!
      };
    }
  } else if (options.config != null) {
    const file = await fs.readFile(options.config, 'utf-8');
    const parsed = await yaml.parse(file);
    const keys = (parsed.client_auth?.jwks?.keys ?? []).filter((key: any) => key.alg == 'HS256');
    if (keys.length == 0) {
      throw new Error('No HS256 key found in the config');
    }

    let endpoint = options.endpoint;
    if (endpoint == null) {
      endpoint = `http://127.0.0.1:${parsed.port ?? 8080}`;
    }

    const aud = [parsed.client_auth?.audience?.[0], endpoint].filter((a) => a != null);

    const rawKey = keys[0];
    const key = await jose.importJWK(rawKey);

    const sub = options.sub ?? 'test_user';

    const token = await new jose.SignJWT({})
      .setProtectedHeader({ alg: rawKey.alg, kid: rawKey.kid })
      .setSubject(sub)
      .setIssuedAt()
      .setIssuer('test-client')
      .setAudience(aud)
      .setExpirationTime('24h')
      .sign(key);

    return { token, endpoint };
  } else {
    throw new Error(`Specify token or config path`);
  }
}
