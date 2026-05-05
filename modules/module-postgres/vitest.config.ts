import { serviceIntegrationTestConfig } from '../test_config';

const baseConfig = serviceIntegrationTestConfig(__dirname);
baseConfig.resolve = {
  ...baseConfig.resolve,
  alias: {
    ...baseConfig.resolve?.alias
  }
};
export default baseConfig;
