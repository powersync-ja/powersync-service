import { utils } from '@powersync/lib-services-framework';

export const env = utils.collectEnvironmentVariables({
  CI: utils.type.boolean.default('false')
});
