import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import Fastify, { FastifyInstance } from 'fastify';
import { container } from '@powersync/lib-services-framework';
import * as auth from '../../../src/routes/auth.js';
import * as system from '../../../src/system/system-index.js';
import { configureFastifyServer } from '../../../src/index.js';
import { ProbeRoutes } from '../../../src/routes/endpoints/probes.js';

vi.mock("@powersync/lib-services-framework", async () => {
  const actual = await vi.importActual("@powersync/lib-services-framework") as any;
  return {
    ...actual,
    container: {
      ...actual.container,
      probes: {
        state: vi.fn()
      }
    },
  }
})

describe('Probe Routes Integration', () => {
  let app: FastifyInstance;
  let mockSystem: system.CorePowerSyncSystem;

  beforeEach(async () => {
    app = Fastify();
    mockSystem = {} as system.CorePowerSyncSystem;
    await configureFastifyServer(app, { system: mockSystem });
    await app.ready();
  });

  afterEach(async () => {
    await app.close();
  });

  describe('Startup Probe', () => {
    it('returns 200 when system is started', async () => {
      const mockState = {
        started: true,
        ready: true,
        touched_at: new Date()
      };
      vi.spyOn(auth, 'authUser').mockResolvedValue({ authorized: true });
      vi.mocked(container.probes.state).mockReturnValue(mockState);

      const response = await app.inject({
        method: 'GET',
        url: ProbeRoutes.STARTUP,
      });

      expect(response.statusCode).toBe(200);
      expect(JSON.parse(response.payload)).toEqual({
        ...mockState,
        touched_at: mockState.touched_at.toISOString()
      });
    });

    it('returns 400 when system is not started', async () => {
      const mockState = {
        started: false,
        ready: false,
        touched_at: new Date()
      };

      vi.mocked(container.probes.state).mockReturnValue(mockState);

      const response = await app.inject({
        method: 'GET',
        url: ProbeRoutes.STARTUP,
      });

      expect(response.statusCode).toBe(400);
      expect(JSON.parse(response.payload)).toEqual({
        ...mockState,
        touched_at: mockState.touched_at.toISOString()
      });
    });
  });

  describe('Liveness Probe', () => {
    it('returns 200 when system was touched recently', async () => {
      const mockState = {
        started: true,
        ready: true,
        touched_at: new Date()
      };

      vi.mocked(container.probes.state).mockReturnValue(mockState);

      const response = await app.inject({
        method: 'GET',
        url: ProbeRoutes.LIVENESS,
      });

      expect(response.statusCode).toBe(200);
      expect(JSON.parse(response.payload)).toEqual({
        ...mockState,
        touched_at: mockState.touched_at.toISOString()
      });
    });

    it('returns 400 when system has not been touched recently', async () => {
      const mockState = {
        started: true,
        ready: true,
        touched_at: new Date(Date.now() - 15000) // 15 seconds ago
      };

      vi.mocked(container.probes.state).mockReturnValue(mockState);

      const response = await app.inject({
        method: 'GET',
        url: ProbeRoutes.LIVENESS,
      });

      expect(response.statusCode).toBe(400);
      expect(JSON.parse(response.payload)).toEqual({
        ...mockState,
        touched_at: mockState.touched_at.toISOString()
      });
    });
  });

  describe('Readiness Probe', () => {
    it('returns 200 when system is ready', async () => {
      const mockState = {
        started: true,
        ready: true,
        touched_at: new Date()
      };

      vi.mocked(container.probes.state).mockReturnValue(mockState);

      const response = await app.inject({
        method: 'GET',
        url: ProbeRoutes.READINESS,
      });

      expect(response.statusCode).toBe(200);
      expect(JSON.parse(response.payload)).toEqual({
        ...mockState,
        touched_at: mockState.touched_at.toISOString()
      });
    });

    it('returns 400 when system is not ready', async () => {
      const mockState = {
        started: true,
        ready: false,
        touched_at: new Date()
      };

      vi.mocked(container.probes.state).mockReturnValue(mockState);

      const response = await app.inject({
        method: 'GET',
        url: ProbeRoutes.READINESS,
      });

      expect(response.statusCode).toBe(400);
      expect(JSON.parse(response.payload)).toEqual({
        ...mockState,
        touched_at: mockState.touched_at.toISOString()
      });
    });
  });

  describe('Request Queue Behavior', () => {
    it('handles concurrent requests within limits', async () => {
      const mockState = { started: true, ready: true, touched_at: new Date() };
      vi.mocked(container.probes.state).mockReturnValue(mockState);

      // Create array of 15 concurrent requests (default concurrency is 10)
      const requests = Array(15).fill(null).map(() =>
        app.inject({
          method: 'GET',
          url: ProbeRoutes.STARTUP,
        })
      );

      const responses = await Promise.all(requests);

      // All requests should complete successfully
      responses.forEach(response => {
        expect(response.statusCode).toBe(200);
        expect(JSON.parse(response.payload)).toEqual({
          ...mockState,
          touched_at: mockState.touched_at.toISOString()
        });
      });
    });

    it('respects max queue depth', async () => {
      const mockState = { started: true, ready: true, touched_at: new Date() };
      vi.mocked(container.probes.state).mockReturnValue(mockState);

      // Create array of 35 concurrent requests (default max_queue_depth is 20)
      const requests = Array(35).fill(null).map(() =>
        app.inject({
          method: 'GET',
          url: ProbeRoutes.STARTUP,
        })
      );

      const responses = await Promise.all(requests);

      // Some requests should succeed and some should fail with 429
      const successCount = responses.filter(r => r.statusCode === 200).length;
      const queueFullCount = responses.filter(r => r.statusCode === 429).length;

      expect(successCount).toBeGreaterThan(0);
      expect(queueFullCount).toBeGreaterThan(0);
      expect(successCount + queueFullCount).toBe(35);
    });
  });

  describe('Content Types', () => {
    it('returns correct content type headers', async () => {
      const mockState = { started: true, ready: true, touched_at: new Date() };
      vi.mocked(container.probes.state).mockReturnValue(mockState);

      const response = await app.inject({
        method: 'GET',
        url: ProbeRoutes.STARTUP,
      });

      expect(response.headers['content-type']).toMatch(/application\/json/);
    });
  });
});
