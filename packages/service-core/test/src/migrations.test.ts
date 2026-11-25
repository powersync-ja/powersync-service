import { AbstractPowerSyncMigrationAgent } from '@/index.js';
import fs from 'fs/promises';
import os from 'os';
import path from 'path';
import { afterEach, describe, expect, it, vi } from 'vitest';

class DirectoryMigrationAgent extends AbstractPowerSyncMigrationAgent {
  constructor(private dir: string) {
    super();
  }

  getInternalScriptsDir(): string {
    return this.dir;
  }

  get store(): never {
    throw new Error('not used');
  }

  get locks(): never {
    throw new Error('not used');
  }

  async [Symbol.asyncDispose]() {}
}

describe('loadInternalMigrations', () => {
  afterEach(() => {
    vi.restoreAllMocks();
  });

  it('returns migrations sorted by name, whatever order readdir lists them in', async () => {
    const dir = await fs.mkdtemp(path.join(os.tmpdir(), 'powersync-migrations-'));
    try {
      const names = ['1684951997326-init', '1688556755264-initial-sync-rules', '1702295701188-sync-rule-state'];
      for (const name of names) {
        await fs.writeFile(path.join(dir, `${name}.js`), 'export const up = async () => {};\n');
      }
      await fs.writeFile(path.join(dir, 'README.md'), '');

      // fs.readdir does not promise any order. Node happens to sort on common filesystems; other
      // runtimes and filesystems return directory order, so list the files backwards here.
      const readdir = fs.readdir;
      vi.spyOn(fs, 'readdir').mockImplementation((async (dirPath: string) => {
        const files = (await readdir(dirPath)) as string[];
        return [...files].sort().reverse();
      }) as typeof fs.readdir);

      const migrations = await new DirectoryMigrationAgent(dir).loadInternalMigrations();

      expect(fs.readdir).toHaveBeenCalled();
      expect(migrations.map((m) => m.name)).toEqual(names);
    } finally {
      await fs.rm(dir, { recursive: true, force: true });
    }
  });
});
