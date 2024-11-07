import { describe, it, expect, vi, beforeEach } from 'vitest';
import { container } from '@powersync/lib-services-framework';
import { startupCheck, livenessCheck, readinessCheck } from '../../../src/routes/endpoints/probes.js';

// Mock the container
vi.mock('@powersync/lib-services-framework', () => ({
  container: {
    probes: {
      state: vi.fn()
    }
  },
  router: {
    HTTPMethod: {
      GET: 'GET'
    },
    RouterResponse: class RouterResponse {
      status: number;
      data: any;
      headers: Record<string, string>;
      afterSend: () => Promise<void>;
      __micro_router_response = true;

      constructor({ status, data, headers, afterSend }: {
        status?: number;
        data: any;
        headers?: Record<string, string>;
        afterSend?: () => Promise<void>;
      }) {
        this.status = status || 200;
        this.data = data;
        this.headers = headers || { 'Content-Type': 'application/json' };
        this.afterSend = afterSend ?? (() => Promise.resolve());
      }
    }
  }
}));

describe('Probe Routes', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  describe('startupCheck', () => {
    it('has the correct route definitions', () => {
      expect(startupCheck.path).toBe('/probes/startup');
      expect(startupCheck.method).toBe('GET');
    });

    it('returns 200 when started is true', async () => {
      const mockState = {
        started: true,
        ready: true,
        touched_at: new Date()
      };

      vi.mocked(container.probes.state).mockReturnValue(mockState);

      const response = await startupCheck.handler();

      expect(response.status).toEqual(200);
      expect(response.data).toEqual(mockState);
    });

    it('returns 400 when started is false', async () => {
      const mockState = {
        started: false,
        ready: false,
        touched_at: new Date()
      };

      vi.mocked(container.probes.state).mockReturnValue(mockState);

      const response = await startupCheck.handler();

      expect(response.status).toBe(400);
      expect(response.data).toEqual(mockState);
    });
  });

  describe('livenessCheck', () => {
    it('has the correct route definitions', () => {
      expect(livenessCheck.path).toBe('/probes/liveness');
      expect(livenessCheck.method).toBe('GET');
    });

    it('returns 200 when last touch was more than 10 seconds ago', async () => {
      const mockState = {
        started: true,
        ready: true,
        touched_at: new Date(Date.now() - 11000) // 11 seconds ago
      };

      vi.mocked(container.probes.state).mockReturnValue(mockState);

      const response = await livenessCheck.handler();

      expect(response.status).toBe(200);
      expect(response.data).toEqual(mockState);
    });

    it('returns 400 when last touch was less than 10 seconds ago', async () => {
      const mockState = {
        started: true,
        ready: true,
        touched_at: new Date(Date.now() - 5000)
      };

      vi.mocked(container.probes.state).mockReturnValue(mockState);

      const response = await livenessCheck.handler();

      expect(response.status).toBe(400);
      expect(response.data).toEqual(mockState);
    });
  });

  describe('readinessCheck', () => {
    it('has the correct route definitions', () => {
      expect(readinessCheck.path).toBe('/probes/readiness');
      expect(readinessCheck.method).toBe('GET');
    });

    it('returns 200 when ready is true', async () => {
      const mockState = {
        started: true,
        ready: true,
        touched_at: new Date()
      };

      vi.mocked(container.probes.state).mockReturnValue(mockState);

      const response = await readinessCheck.handler();

      expect(response.status).toBe(200);
      expect(response.data).toEqual(mockState);
    });

    it('returns 400 when ready is false', async () => {
      const mockState = {
        started: true,
        ready: false,
        touched_at: new Date()
      };

      vi.mocked(container.probes.state).mockReturnValue(mockState);

      const response = await readinessCheck.handler();

      expect(response.status).toBe(400);
      expect(response.data).toEqual(mockState);
    });
  });
});
