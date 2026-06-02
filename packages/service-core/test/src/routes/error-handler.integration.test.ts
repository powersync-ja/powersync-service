import { errors } from '@powersync/lib-services-framework';
import Fastify, { FastifyInstance } from 'fastify';
import { Readable } from 'node:stream';
import * as zlib from 'node:zlib';
import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { registerFastifyErrorHandler } from '../../../src/routes/route-register.js';

describe('Fastify error handler', () => {
  let app: FastifyInstance;

  beforeEach(() => {
    app = Fastify();
    registerFastifyErrorHandler(app);
  });

  afterEach(async () => {
    await app.close();
  });

  describe('errors thrown in route before sending a response', () => {
    it('returns a JSON error body with no content-encoding when none was set', async () => {
      app.get('/boom', async () => {
        throw new errors.ServiceError({
          status: 503,
          code: 'PSYNC_S2003' as any,
          description: 'Service unavailable'
        });
      });
      await app.ready();

      const response = await app.inject({ method: 'GET', url: '/boom' });

      expect(response.statusCode).toBe(503);
      expect(response.headers['content-type']).toMatch(/application\/json/);
      expect(response.headers['content-encoding']).toBeUndefined();
      expect(JSON.parse(response.payload)).toMatchObject({
        error: { code: 'PSYNC_S2003', status: 503 }
      });
    });

    it('gzip-encodes the error body when content-encoding=gzip was set before the throw', async () => {
      app.get('/boom', async (_request, reply) => {
        reply.header('content-encoding', 'gzip');
        throw new Error('kaboom');
      });
      await app.ready();

      const response = await app.inject({ method: 'GET', url: '/boom' });

      expect(response.statusCode).toBe(500);
      expect(response.headers['content-encoding']).toBe('gzip');
      const decoded = zlib.gunzipSync(response.rawPayload).toString('utf8');
      expect(JSON.parse(decoded).error.code).toBe('PSYNC_S2001');
    });

    it('zstd-encodes the error body when content-encoding=zstd was set before the throw', async () => {
      app.get('/boom', async (_request, reply) => {
        reply.header('content-encoding', 'zstd');
        throw new Error('kaboom');
      });
      await app.ready();

      const response = await app.inject({ method: 'GET', url: '/boom' });

      expect(response.statusCode).toBe(500);
      expect(response.headers['content-encoding']).toBe('zstd');
      const decoded = zlib.zstdDecompressSync(response.rawPayload).toString('utf8');
      expect(JSON.parse(decoded).error.code).toBe('PSYNC_S2001');
    });
  });

  describe('errors after responding with a stream, before any data is sent', () => {
    it('still returns a JSON error response with no encoding', async () => {
      app.get('/stream', async (_request, reply) => {
        const stream = new Readable({
          read() {
            process.nextTick(() => this.destroy(new Error('pre-data failure')));
          }
        });
        reply.header('content-type', 'application/x-ndjson');
        return reply.send(stream);
      });
      await app.ready();

      const response = await app.inject({ method: 'GET', url: '/stream' });

      expect(response.statusCode).toBe(500);
      expect(response.headers['content-encoding']).toBeUndefined();
      expect(response.headers['content-type']).toMatch(/application\/json/);
      expect(JSON.parse(response.payload).error.code).toBe('PSYNC_S2001');
    });

    it('still returns a gzip-encoded JSON error response when encoding was set first', async () => {
      app.get('/stream', async (_request, reply) => {
        reply.header('content-encoding', 'gzip');
        reply.header('content-type', 'application/x-ndjson');
        const stream = new Readable({
          read() {
            process.nextTick(() => this.destroy(new Error('pre-data failure')));
          }
        });
        return reply.send(stream);
      });
      await app.ready();

      const response = await app.inject({ method: 'GET', url: '/stream' });

      expect(response.statusCode).toBe(500);
      expect(response.headers['content-encoding']).toBe('gzip');
      const decoded = zlib.gunzipSync(response.rawPayload).toString('utf8');
      expect(JSON.parse(decoded).error.code).toBe('PSYNC_S2001');
    });
  });

  describe('errors after a stream has sent data', () => {
    it('lets fastify tear the response down without raising an uncaught exception', async () => {
      app.get('/stream', async (_request, reply) => {
        let chunks = 0;
        const stream = new Readable({
          read() {
            if (chunks++ === 0) {
              this.push('first chunk\n');
            } else {
              this.destroy(new Error('mid-stream failure'));
            }
          }
        });
        reply.header('content-type', 'application/x-ndjson');
        return reply.send(stream);
      });
      await app.ready();

      const uncaught: Error[] = [];
      const onUncaught = (err: Error) => uncaught.push(err);
      const onUnhandled = (err: any) => uncaught.push(err);
      process.on('uncaughtException', onUncaught);
      process.on('unhandledRejection', onUnhandled);

      try {
        await expect(app.inject({ method: 'GET', url: '/stream' })).rejects.toThrow(/destroyed/);
        await new Promise((resolve) => setImmediate(resolve));
      } finally {
        process.off('uncaughtException', onUncaught);
        process.off('unhandledRejection', onUnhandled);
      }

      expect(uncaught).toEqual([]);
    });
  });

  describe('handler inheritance into child scopes', () => {
    it('handles errors thrown in routes registered inside a child scope', async () => {
      await app.register(async (child) => {
        child.get('/boom', async () => {
          throw new Error('child scope failure');
        });
      });
      await app.ready();

      const response = await app.inject({ method: 'GET', url: '/boom' });

      expect(response.statusCode).toBe(500);
      expect(response.headers['content-type']).toMatch(/application\/json/);
      const body = JSON.parse(response.payload);
      expect(body.error?.code).toBe('PSYNC_S2001');
    });
  });

  describe('built-in Fastify errors', () => {
    it('preserves the 400 status code for an invalid JSON body', async () => {
      app.post('/echo', async (request) => ({ received: request.body }));
      await app.ready();

      const response = await app.inject({
        method: 'POST',
        url: '/echo',
        headers: { 'content-type': 'application/json' },
        payload: '{ not json'
      });

      expect(response.statusCode).toBe(400);
      const body = JSON.parse(response.payload);
      expect(body.error.status).toBe(400);
      expect(body.error.code).toBe('FST_ERR_CTP_INVALID_JSON_BODY');
    });

    it('preserves the 400 status code for an empty JSON body', async () => {
      app.post('/echo', async (request) => ({ received: request.body }));
      await app.ready();

      const response = await app.inject({
        method: 'POST',
        url: '/echo',
        headers: { 'content-type': 'application/json' },
        payload: ''
      });

      expect(response.statusCode).toBe(400);
      const body = JSON.parse(response.payload);
      expect(body.error.status).toBe(400);
      expect(body.error.code).toBe('FST_ERR_CTP_EMPTY_JSON_BODY');
    });

    it('preserves the 400 status code for a schema validation failure', async () => {
      app.post(
        '/schema',
        {
          schema: {
            body: {
              type: 'object',
              required: ['name'],
              properties: { name: { type: 'string' } }
            }
          }
        },
        async () => ({ ok: true })
      );
      await app.ready();

      const response = await app.inject({
        method: 'POST',
        url: '/schema',
        headers: { 'content-type': 'application/json' },
        payload: JSON.stringify({})
      });

      expect(response.statusCode).toBe(400);
      const body = JSON.parse(response.payload);
      expect(body.error.status).toBe(400);
      expect(body.error.code).toBe('FST_ERR_VALIDATION');
    });

    it('preserves the 413 status code when the body exceeds the limit', async () => {
      const tinyApp = Fastify({ bodyLimit: 16 });
      registerFastifyErrorHandler(tinyApp);
      tinyApp.post('/echo', async (request) => ({ received: request.body }));
      await tinyApp.ready();

      try {
        const response = await tinyApp.inject({
          method: 'POST',
          url: '/echo',
          headers: { 'content-type': 'application/json' },
          payload: JSON.stringify({ data: 'x'.repeat(64) })
        });

        expect(response.statusCode).toBe(413);
        const body = JSON.parse(response.payload);
        expect(body.error.status).toBe(413);
        expect(body.error.code).toBe('FST_ERR_CTP_BODY_TOO_LARGE');
      } finally {
        await tinyApp.close();
      }
    });

    it('preserves the 415 status code for an unsupported media type', async () => {
      app.post('/json-only', async (request) => ({ received: request.body }));
      // Drop the default text/plain parser so non-JSON content types are rejected.
      app.removeContentTypeParser('text/plain');
      await app.ready();

      const response = await app.inject({
        method: 'POST',
        url: '/json-only',
        headers: { 'content-type': 'text/plain' },
        payload: 'hello'
      });

      expect(response.statusCode).toBe(415);
      const body = JSON.parse(response.payload);
      expect(body.error.status).toBe(415);
      expect(body.error.code).toBe('FST_ERR_CTP_INVALID_MEDIA_TYPE');
    });
  });
});
