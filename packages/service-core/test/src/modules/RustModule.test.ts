import { logger } from '@powersync/lib-services-framework';
import { expect, vi } from 'vitest';

import { modules } from '@/index.js';
import { rustModuleTest } from '@powersync/service-core-tests';

rustModuleTest('can initialize rust module', async ({ rustAddonPath }) => {
  // The module's child logger inherits log() from the shared logger, so the Rust side binds this spy.
  const log = vi.spyOn(logger, 'log');

  const module = new modules.RustModule({ name: 'Rust', addonPath: rustAddonPath });
  await module.loadRust();

  // Log events are forwarded asynchronously through a threadsafe function, so we can't inspect them synchronously.
  await vi.waitFor(() => {
    expect(log).toHaveBeenCalledWith(
      expect.objectContaining({
        level: 'info',
        message: 'Initializing Rust module for PowerSync Open Edition'
      })
    );
  });
});
