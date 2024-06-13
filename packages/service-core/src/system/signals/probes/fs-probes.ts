import * as fs from 'node:fs/promises';
import * as path from 'node:path';

import { createInMemoryProbe } from './memory-probes.js';
import { ProbeModule } from './probes.js';

export const createFSProbe = (dir: string = path.join(process.cwd(), '.probes')): ProbeModule => {
  const memory_probe = createInMemoryProbe();

  const startup_path = path.join(dir, 'startup');
  const readiness_path = path.join(dir, 'ready');
  /**
   * Note this is different to Journey Micro.
   * Updated here from `alive` to `poll` as per previous comment:
   *  FIXME: The above probe touches the wrong file
   *  */
  const liveness_path = path.join(dir, 'poll');

  const touchFile = async (path: string) => {
    try {
      await fs.mkdir(dir, {
        recursive: true
      });
      await fs.writeFile(path, `${Date.now()}`);
    } catch (err) {}
  };

  return {
    poll_timeout: memory_probe.poll_timeout,
    state: memory_probe.state,

    async ready() {
      await Promise.all([touchFile(startup_path), touchFile(readiness_path)]);
      await memory_probe.ready();
    },
    async unready() {
      try {
        await fs.unlink(readiness_path);
      } catch (err) {}
      await memory_probe.unready();
    },
    async touch() {
      await touchFile(liveness_path);
      await memory_probe.touch();
    }
  };
};
