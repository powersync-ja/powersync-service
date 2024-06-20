import { ProbeModule, ProbeState } from './probes.js';

export type ProbeParams = {
  poll_timeout_ms: number;
};

export const createInMemoryProbe = (params?: ProbeParams): ProbeModule => {
  const state: ProbeState = {
    ready: false,
    started: false,
    touched_at: new Date()
  };

  return {
    poll_timeout: params?.poll_timeout_ms ?? 10000,

    state: () => {
      return {
        ready: state.ready,
        started: state.started,
        touched_at: state.touched_at
      };
    },
    ready: async () => {
      state.ready = true;
      state.started = true;
      state.touched_at = new Date();
    },
    unready: async () => {
      state.ready = false;
    },
    touch: async () => {
      state.touched_at = new Date();
    }
  };
};
