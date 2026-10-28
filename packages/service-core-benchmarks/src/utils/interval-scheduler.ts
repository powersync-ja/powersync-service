import { IntervalScheduler } from '../types/BenchmarkResource.js';

export const systemIntervalScheduler: IntervalScheduler = {
  set(callback, intervalMs) {
    return setInterval(callback, intervalMs);
  },
  clear(handle) {
    clearInterval(handle as ReturnType<typeof setInterval>);
  }
};
