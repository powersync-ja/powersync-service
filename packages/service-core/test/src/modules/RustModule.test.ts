import { logger } from '@powersync/lib-services-framework';
import { expect, test, vi } from 'vitest';

import { rustModuleForTesting } from '@powersync/service-core-tests';

test('can initialize rust module', async () => {
  // The module's child logger inherits log() from the shared logger, so the Rust side binds this spy.
  const log = vi.spyOn(logger, 'log');

  const module = await rustModuleForTesting();
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
