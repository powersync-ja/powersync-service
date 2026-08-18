import { serviceIntegrationTestConfig } from '../test_config';

const config = serviceIntegrationTestConfig(__dirname);
config.test ??= {};
config.test.testTimeout = 30_000;

export default config;
