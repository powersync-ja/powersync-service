import { logger } from '@powersync/lib-services-framework';

import { POWERSYNC_VERSION } from '@powersync/service-core';

export function logBooting(runner: string) {
  const version = POWERSYNC_VERSION;
  const edition = 'Open Edition';
  logger.info(`Booting PowerSync Service v${version}, ${runner}, ${edition}`, { version, edition, runner });
}
