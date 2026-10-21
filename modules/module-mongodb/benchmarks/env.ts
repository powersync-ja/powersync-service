import { utils } from '@powersync/lib-services-framework';

const POSITIVE_INTEGER = utils.type.string
  .transform(Number)
  .refine((value) => Number.isSafeInteger(value) && value > 0, 'Must be a positive safe integer');

export const ENV = utils.collectEnvironmentVariables({
  MEMORY_EAGER: utils.type.boolean.default('false'),
  THROUGHPUT_ROWS: POSITIVE_INTEGER.default('2000'),
  THROUGHPUT_PAYLOAD_BYTES: POSITIVE_INTEGER.default('1048576')
});
