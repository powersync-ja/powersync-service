import * as framework from '@powersync/lib-services-framework';
import * as system from '../system/system-index.js';

export const ensureAutomaticMigrations = async (options: { serviceContext: system.ServiceContext }) => {
  const { serviceContext } = options;
  if (serviceContext.configuration.migrations?.disable_auto_migration) {
    return;
  }
  await serviceContext.migrations.migrate({
    direction: framework.migrations.Direction.Up,
    migrationContext: {
      service_context: serviceContext
    }
  });
};
