import path from 'node:path';
import { serviceIntegrationTestConfig } from '../test_config';

const baseConfig = serviceIntegrationTestConfig(__dirname);
baseConfig.resolve = {
  ...baseConfig.resolve,
  alias: {
    ...baseConfig.resolve?.alias,
    '@testing-convex': path.resolve(__dirname, 'convex')
  }
};
export default baseConfig;
