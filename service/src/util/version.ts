import { logger } from '@powersync/lib-services-framework';

import pkg from '@powersync/service-core/package.json' with { type: 'json' };

export function logBooting(runner: string) {
  const version = pkg.version;
  const edition = 'Community Edition';
  logger.info(`Booting PowerSync Service v${version}, ${runner}, ${edition}`, { version, edition, runner });
}
