import * as https from 'https';
import * as http from 'http';
import * as dns from 'dns/promises';
import ip from 'ipaddr.js';
import * as jose from 'jose';
import * as net from 'net';
import fetch from 'node-fetch';

import { KeySpec } from './KeySpec.js';
import { KeyCollector, KeyResult } from './KeyCollector.js';

export type RemoteJWKSCollectorOptions = {
  /**
   * Blocks IP Ranges from the BLOCKED_IP_RANGES array
   */
  block_local_ip?: boolean;
};

/**
 * Set of keys fetched from JWKS URI.
 */
export class RemoteJWKSCollector implements KeyCollector {
  private url: URL;

  constructor(
    url: string,
    protected options?: RemoteJWKSCollectorOptions
  ) {
    try {
      this.url = new URL(url);
    } catch (e) {
      throw new Error(`Invalid jwks_uri: ${url}`);
    }

    // We do support http here for self-hosting use cases.
    // Management service restricts this to https for hosted versions.
    if (this.url.protocol != 'https:' && this.url.protocol != 'http:') {
      throw new Error(`Only http(s) is supported for jwks_uri, got: ${url}`);
    }
  }

  async getKeys(): Promise<KeyResult> {
    const abortController = new AbortController();
    const timeout = setTimeout(() => {
      abortController.abort();
    }, 30_000);

    const res = await fetch(this.url, {
      method: 'GET',
      headers: {
        Accept: 'application/json'
      },
      signal: abortController.signal,
      agent: await this.resolveAgent()
    });

    if (!res.ok) {
      throw new jose.errors.JWKSInvalid(`JWKS request failed with ${res.statusText}`);
    }

    const data = (await res.json()) as any;

    clearTimeout(timeout);

    // https://github.com/panva/jose/blob/358e864a0cccf1e0f9928a959f91f18f3f06a7de/src/jwks/local.ts#L36
    if (
      data.keys == null ||
      !Array.isArray(data.keys) ||
      !(data.keys as any[]).every((key) => typeof key == 'object' && !Array.isArray(key))
    ) {
      return { keys: [], errors: [new jose.errors.JWKSInvalid(`No keys in found in JWKS response`)] };
    }

    let keys: KeySpec[] = [];
    for (let keyData of data.keys) {
      if (keyData.kty != 'RSA' && keyData.kty != 'OKP' && keyData.kty != 'EC') {
        // HS (oct) keys not allowed because they are symmetric
        continue;
      }

      if (typeof keyData.use == 'string') {
        if (keyData.use != 'sig') {
          continue;
        }
      }
      if (Array.isArray(keyData.key_ops)) {
        if (!keyData.key_ops.includes('verify')) {
          continue;
        }
      }

      const key = await KeySpec.importKey(keyData);
      keys.push(key);
    }

    return { keys: keys, errors: [] };
  }

  /**
   * Resolve IP, and check that it is in an allowed range.
   */
  async resolveAgent(): Promise<http.Agent | https.Agent> {
    const hostname = this.url.hostname;
    let resolved_ip: string;
    if (net.isIPv6(hostname)) {
      throw new Error('IPv6 not supported yet');
    } else if (net.isIPv4(hostname)) {
      // All good
      resolved_ip = hostname;
    } else {
      resolved_ip = (await dns.resolve4(hostname))[0];
    }

    const parsed = ip.parse(resolved_ip);
    if (parsed.kind() != 'ipv4' || (this.options?.block_local_ip && parsed.range() !== 'unicast')) {
      // Do not connect to any reserved IPs, including loopback and private ranges
      throw new Error(`IPs in this range are not supported: ${resolved_ip}`);
    }

    const options = {
      // This is the host that the agent connects to
      host: resolved_ip
    };

    switch (this.url.protocol) {
      case 'http:':
        return new http.Agent(options);
      case 'https:':
        return new https.Agent(options);
    }
    throw new Error('http or or https is required for protocol');
  }
}
