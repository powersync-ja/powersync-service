import { system, utils } from '@powersync/service-core';

export interface ReplicationChildServiceSetup {
  readonly configuration: utils.ResolvedPowerSyncConfig;
  readonly defaultSchema: string;
}

export interface ReplicationChildSourceImplementation {
  getServiceSetup(): ReplicationChildServiceSetup;
  initialize(context: system.ServiceContextContainer): Promise<void>;
}
