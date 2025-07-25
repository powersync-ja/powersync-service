import * as jose from 'jose';

export const HS_ALGORITHMS = ['HS256', 'HS384', 'HS512'];
export const RSA_ALGORITHMS = ['RS256', 'RS384', 'RS512'];
export const EC_ALGORITHMS = ['ES256', 'ES384', 'ES512'];
export const OKP_ALGORITHMS = ['EdDSA'];
export const SUPPORTED_ALGORITHMS = [...HS_ALGORITHMS, ...RSA_ALGORITHMS, ...EC_ALGORITHMS, ...OKP_ALGORITHMS];

export interface KeyOptions {
  /**
   * If configured, JWTs verified by this key must have one of these audiences
   * in the `aud` claim, instead of the default.
   */
  requiresAudience?: string[];

  /**
   * If configured, JWTs verified by this key can have a maximum lifetime up to
   * this value, instead of the default.
   */
  maxLifetimeSeconds?: number;
}

export class KeySpec {
  key: jose.KeyLike;
  source: jose.JWK;
  options: KeyOptions;

  static async importKey(key: jose.JWK, options?: KeyOptions): Promise<KeySpec> {
    const parsed = (await jose.importJWK(key)) as jose.KeyLike;
    return new KeySpec(key, parsed, options);
  }

  constructor(source: jose.JWK, key: jose.KeyLike, options?: KeyOptions) {
    this.source = source;
    this.key = key;
    this.options = options ?? {};
  }

  get kid(): string | undefined {
    return this.source.kid;
  }

  get description(): string {
    let details: string[] = [];
    details.push(`kid: ${this.kid ?? '*'}`);
    details.push(`kty: ${this.source.kty}`);
    if (this.source.alg != null) {
      details.push(`alg: ${this.source.alg}`);
    }
    if (this.options.requiresAudience != null) {
      details.push(`aud: ${this.options.requiresAudience.join(', ')}`);
    }

    return `<${details.filter((x) => x != null).join(', ')}>`;
  }

  matchesAlgorithm(jwtAlg: string): boolean {
    if (this.source.alg) {
      return jwtAlg === this.source.alg;
    } else if (this.source.kty === 'RSA') {
      return RSA_ALGORITHMS.includes(jwtAlg);
    } else if (this.source.kty === 'oct') {
      return HS_ALGORITHMS.includes(jwtAlg);
    } else if (this.source.kty === 'OKP') {
      return OKP_ALGORITHMS.includes(jwtAlg);
    } else if (this.source.kty === 'EC') {
      return EC_ALGORITHMS.includes(jwtAlg);
    }

    return false;
  }

  async isValidSignature(token: string): Promise<boolean> {
    try {
      await jose.compactVerify(token, this.key);
      return true;
    } catch (e) {
      if (e.code === 'ERR_JWS_SIGNATURE_VERIFICATION_FAILED') {
        return false;
      } else {
        // Token format error most likely
        throw e;
      }
    }
  }
}
