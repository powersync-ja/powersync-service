import { logger } from '@powersync/lib-services-framework';

export function logBooting(runner: string) {
  const version = process.env.POWERSYNC_VERSION ?? '-dev';
  const isCloud = process.env.MICRO_SERVICE_NAME == 'powersync';
  const edition = isCloud ? 'Cloud Edition' : 'Enterprise Edition';
  logger.info(`Booting PowerSync Service v${version}, ${runner}, ${edition}`, { version, edition, runner });
}
