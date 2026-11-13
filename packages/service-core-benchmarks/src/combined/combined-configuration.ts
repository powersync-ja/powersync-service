import { auth, utils } from '@powersync/service-core';
import { createConfiguration } from '../utils/api-utils.js';

export function createCombinedBenchmarkConfiguration(
  sourceConfiguration: utils.ResolvedPowerSyncConfig,
  port: number,
  clientKeyStore: auth.KeyStore
): utils.ResolvedPowerSyncConfig {
  const httpConfiguration = createConfiguration(port, clientKeyStore);
  return {
    ...sourceConfiguration,
    port: httpConfiguration.port,
    client_keystore: httpConfiguration.client_keystore,
    api_tokens: httpConfiguration.api_tokens,
    jwt_audiences: httpConfiguration.jwt_audiences,
    token_max_expiration: httpConfiguration.token_max_expiration,
    api_parameters: httpConfiguration.api_parameters
  };
}
