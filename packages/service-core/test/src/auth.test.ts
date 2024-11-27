import { CachedKeyCollector } from '@/auth/CachedKeyCollector.js';
import { KeyResult } from '@/auth/KeyCollector.js';
import { KeySpec } from '@/auth/KeySpec.js';
import { KeyStore } from '@/auth/KeyStore.js';
import { RemoteJWKSCollector } from '@/auth/RemoteJWKSCollector.js';
import { StaticKeyCollector } from '@/auth/StaticKeyCollector.js';
import * as jose from 'jose';
import { describe, expect, test } from 'vitest';

const publicKey: jose.JWK = {
  use: 'sig',
  kty: 'RSA',
  e: 'AQAB',
  kid: 'f2e82732b971a135cf1416e8b46dae04d80894e7',
  alg: 'RS256',
  n: 'v7wW9w5vzvACxbo2Ldqt0IHBy0LCloQvnfIr-nhEKmqgBJeBgF2cZSGz0Fe-grRaAvhhDrxaOft2JvZlbUM8vFFnxx-52dYViDBxv8vDxmV1HeEGV69DYrGxnsOLrHQWKkPSeyxtidiwGrNVYuyC21PG1heScTYppxVSHBUh_D9di56ql16Xytv97FJHeBtEYUmyzLQsWGhfzuLBwYSSuZxia4p3-azlztHisht4Ai1KYpX0HWLjh9NGIMzimk2cNdKZjIO1Mm4Tu5S1z9dCauZdocpE5csFHyeLHY3oeXFNgl9GanyM9IAg-0T5QLJA-C6M9lUO4WuVmOtLM_iGlw'
};

const sharedKey: jose.JWK = {
  kid: 'k1',
  alg: 'HS256',
  kty: 'oct',
  k: Buffer.from('mysecret1', 'utf-8').toString('base64url')
};

const sharedKey2: jose.JWK = {
  alg: 'HS256',
  kty: 'oct',
  k: Buffer.from('mysecret2', 'utf-8').toString('base64url')
};

describe('JWT Auth', () => {
  test('KeyStore basics', async () => {
    const keys = await StaticKeyCollector.importKeys([sharedKey]);
    const store = new KeyStore(keys);
    const signKey = (await jose.importJWK(sharedKey)) as jose.KeyLike;
    const signedJwt = await new jose.SignJWT({})
      .setProtectedHeader({ alg: 'HS256', kid: 'k1' })
      .setSubject('f1')
      .setIssuedAt()
      .setIssuer('tester')
      .setAudience('tests')
      .setExpirationTime('5m')
      .sign(signKey);

    const verified = await store.verifyJwt(signedJwt, {
      defaultAudiences: ['tests'],
      maxAge: '6m'
    });
    expect(verified.sub).toEqual('f1');
    await expect(
      store.verifyJwt(signedJwt, {
        defaultAudiences: ['other'],
        maxAge: '6m'
      })
    ).rejects.toThrow('unexpected "aud" claim value');

    await expect(
      store.verifyJwt(signedJwt, {
        defaultAudiences: [],
        maxAge: '6m'
      })
    ).rejects.toThrow('unexpected "aud" claim value');

    await expect(
      store.verifyJwt(signedJwt, {
        defaultAudiences: ['tests'],
        maxAge: '1m'
      })
    ).rejects.toThrow('Token must expire in a maximum of');

    const signedJwt2 = await new jose.SignJWT({})
      .setProtectedHeader({ alg: 'HS256', kid: 'k1' })
      .setIssuedAt()
      .setIssuer('tester')
      .setAudience('tests')
      .setExpirationTime('5m')
      .sign(signKey);

    await expect(
      store.verifyJwt(signedJwt2, {
        defaultAudiences: ['tests'],
        maxAge: '5m'
      })
    ).rejects.toThrow('missing required "sub" claim');
  });

  test('Algorithm validation', async () => {
    const keys = await StaticKeyCollector.importKeys([publicKey]);
    const store = new KeyStore(keys);

    // Bad attempt at signing token with rsa public key
    const spoofedKey: jose.JWK = {
      kty: 'oct',
      kid: publicKey.kid!,
      alg: 'HS256',
      k: publicKey.n!
    };
    const signKey = (await jose.importJWK(spoofedKey)) as jose.KeyLike;

    const signedJwt = await new jose.SignJWT({})
      .setProtectedHeader({ alg: 'HS256', kid: publicKey.kid! })
      .setSubject('f1')
      .setIssuedAt()
      .setIssuer('tester')
      .setAudience('tests')
      .setExpirationTime('5m')
      .sign(signKey);

    await expect(
      store.verifyJwt(signedJwt, {
        defaultAudiences: ['tests'],
        maxAge: '6m'
      })
    ).rejects.toThrow('Unexpected token algorithm HS256');
  });

  test('key selection for key with kid', async () => {
    const keys = await StaticKeyCollector.importKeys([publicKey, sharedKey, sharedKey2]);
    const store = new KeyStore(keys);
    const signKey = (await jose.importJWK(sharedKey)) as jose.KeyLike;
    const signKey2 = (await jose.importJWK(sharedKey2)) as jose.KeyLike;

    // No kid
    const signedJwt = await new jose.SignJWT({})
      .setProtectedHeader({ alg: 'HS256' })
      .setSubject('f1')
      .setIssuedAt()
      .setIssuer('tester')
      .setAudience('tests')
      .setExpirationTime('5m')
      .sign(signKey);

    await expect(
      store.verifyJwt(signedJwt, {
        defaultAudiences: ['tests'],
        maxAge: '6m'
      })
    ).rejects.toThrow(
      'Could not find an appropriate key in the keystore. The key is missing or no key matched the token KID'
    );

    // Wrong kid
    const signedJwt2 = await new jose.SignJWT({})
      .setProtectedHeader({ alg: 'HS256', kid: 'other' })
      .setSubject('f1')
      .setIssuedAt()
      .setIssuer('tester')
      .setAudience('tests')
      .setExpirationTime('5m')
      .sign(signKey);

    await expect(
      store.verifyJwt(signedJwt2, {
        defaultAudiences: ['tests'],
        maxAge: '6m'
      })
    ).rejects.toThrow(
      'Could not find an appropriate key in the keystore. The key is missing or no key matched the token KID'
    );

    // No kid, matches sharedKey2
    const signedJwt3 = await new jose.SignJWT({})
      .setProtectedHeader({ alg: 'HS256' })
      .setSubject('f1')
      .setIssuedAt()
      .setIssuer('tester')
      .setAudience('tests')
      .setExpirationTime('5m')
      .sign(signKey2);

    await expect(
      store.verifyJwt(signedJwt3, {
        defaultAudiences: ['tests'],
        maxAge: '6m'
      })
    ).resolves.toMatchObject({ sub: 'f1' });

    // Random kid, matches sharedKey2
    const signedJwt4 = await new jose.SignJWT({})
      .setProtectedHeader({ alg: 'HS256', kid: 'other' })
      .setSubject('f1')
      .setIssuedAt()
      .setIssuer('tester')
      .setAudience('tests')
      .setExpirationTime('5m')
      .sign(signKey2);

    await expect(
      store.verifyJwt(signedJwt4, {
        defaultAudiences: ['tests'],
        maxAge: '6m'
      })
    ).resolves.toMatchObject({ sub: 'f1' });
  });

  test('KeyOptions', async () => {
    const keys = new StaticKeyCollector([
      await KeySpec.importKey(sharedKey, {
        // This overrides the default validation options
        requiresAudience: ['other'],
        maxLifetimeSeconds: 3600
      })
    ]);
    const store = new KeyStore(keys);
    const signKey = (await jose.importJWK(sharedKey)) as jose.KeyLike;
    const signedJwt = await new jose.SignJWT({})
      .setProtectedHeader({ alg: 'HS256', kid: 'k1' })
      .setSubject('f1')
      .setIssuedAt()
      .setIssuer('tester')
      .setAudience('other')
      .setExpirationTime('50m')
      .sign(signKey);

    const verified = await store.verifyJwt(signedJwt, {
      defaultAudiences: ['tests'],
      maxAge: '6m'
    });
    expect(verified.sub).toEqual('f1');

    const signedJwt2 = await new jose.SignJWT({})
      .setProtectedHeader({ alg: 'HS256', kid: 'k1' })
      .setSubject('f1')
      .setIssuedAt()
      .setIssuer('tester')
      .setAudience('tests') // Doesn't match KeyOptions audience
      .setExpirationTime('50m')
      .sign(signKey);

    await expect(
      store.verifyJwt(signedJwt2, {
        defaultAudiences: ['tests'],
        maxAge: '6m'
      })
    ).rejects.toThrow('unexpected "aud" claim value');

    const signedJwt3 = await new jose.SignJWT({})
      .setProtectedHeader({ alg: 'HS256', kid: 'k1' })
      .setSubject('f1')
      .setIssuedAt()
      .setIssuer('tester')
      .setAudience('other')
      .setExpirationTime('70m') // longer than KeyOptions expiration
      .sign(signKey);

    await expect(
      store.verifyJwt(signedJwt3, {
        defaultAudiences: ['tests'],
        maxAge: '6m'
      })
    ).rejects.toThrow('Token must expire in a maximum of');
  });

  test('http', async () => {
    // Not ideal to rely on an external endpoint for tests, but it is good to test that this
    // one actually works.
    const remote = new RemoteJWKSCollector(
      'https://www.googleapis.com/service_accounts/v1/jwk/securetoken@system.gserviceaccount.com'
    );
    const { keys, errors } = await remote.getKeys();
    expect(errors).toEqual([]);
    expect(keys.length).toBeGreaterThanOrEqual(1);

    // The localhost hostname fails to resolve correctly on MacOS https://github.com/nodejs/help/issues/2163
    const invalid = new RemoteJWKSCollector('https://127.0.0.1/.well-known/jwks.json', {
      block_local_ip: true
    });
    expect(invalid.getKeys()).rejects.toThrow('IPs in this range are not supported');
  });

  test('http not blocking local IPs', async () => {
    // Not ideal to rely on an external endpoint for tests, but it is good to test that this
    // one actually works.
    const remote = new RemoteJWKSCollector(
      'https://www.googleapis.com/service_accounts/v1/jwk/securetoken@system.gserviceaccount.com'
    );
    const { keys, errors } = await remote.getKeys();
    expect(errors).toEqual([]);
    expect(keys.length).toBeGreaterThanOrEqual(1);

    // The localhost hostname fails to resolve correctly on MacOS https://github.com/nodejs/help/issues/2163
    const invalid = new RemoteJWKSCollector('https://127.0.0.1/.well-known/jwks.json');
    // Should try and fetch
    expect(invalid.getKeys()).rejects.toThrow('ECONNREFUSED');
  });

  test('caching', async () => {
    let currentResponse: Promise<KeyResult>;

    const cached = new CachedKeyCollector({
      async getKeys() {
        return currentResponse;
      }
    });

    currentResponse = Promise.resolve({
      errors: [],
      keys: [await KeySpec.importKey(publicKey)]
    });

    let key = (await cached.getKeys()).keys[0];
    expect(key.kid).toEqual(publicKey.kid!);

    currentResponse = undefined as any;

    key = (await cached.getKeys()).keys[0];
    expect(key.kid).toEqual(publicKey.kid!);

    cached.addTimeForTests(301_000);
    currentResponse = Promise.reject('refresh failed');

    // Uses the promise, refreshes in the background
    let response = await cached.getKeys();
    expect(response.keys[0].kid).toEqual(publicKey.kid!);
    expect(response.errors).toEqual([]);

    // Wait for refresh to finish
    await cached.addTimeForTests(0);
    response = await cached.getKeys();
    // Still have the cached key, but also have the error
    expect(response.keys[0].kid).toEqual(publicKey.kid!);
    expect(response.errors[0].message).toMatch('Failed to fetch');

    await cached.addTimeForTests(3601_000);
    response = await cached.getKeys();

    // Now the keys have expired, and the request still fails
    expect(response.keys).toEqual([]);
    expect(response.errors[0].message).toMatch('Failed to fetch');

    currentResponse = Promise.resolve({
      errors: [],
      keys: [await KeySpec.importKey(publicKey)]
    });

    // After a delay, we can refresh again
    await cached.addTimeForTests(30_000);
    key = (await cached.getKeys()).keys[0];
    expect(key.kid).toEqual(publicKey.kid!);
  });
});
