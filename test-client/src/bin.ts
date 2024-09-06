import { program } from 'commander';
import { getCheckpointData } from './client.js';
import { getCredentials } from './auth.js';
import * as jose from 'jose';

program
  .command('fetch-operations')
  .option('-t, --token [token]', 'JWT to use for authentication')
  .option('-e, --endpoint [endpoint]', 'endpoint URI')
  .option('-c, --config [config]', 'path to powersync.yaml, to auto-generate a token from a HS256 key')
  .option('-u, --sub [sub]', 'sub field for auto-generated token')
  .option('--raw', 'output operations as received, without normalizing')
  .action(async (options) => {
    const credentials = await getCredentials(options);
    const data = await getCheckpointData({ ...credentials, raw: options.raw });
    console.log(JSON.stringify(data, null, 2));
  });

program
  .command('generate-token')
  .description('Generate a JWT from for a given powersync.yaml config file')
  .option('-c, --config [config]', 'path to powersync.yaml')
  .option('-u, --sub [sub]', 'payload sub')
  .option('-e, --endpoint [endpoint]', 'additional payload aud')
  .action(async (options) => {
    const credentials = await getCredentials(options);
    const decoded = await jose.decodeJwt(credentials.token);

    console.error(`Payload:\n${JSON.stringify(decoded, null, 2)}\nToken:`);
    console.log(credentials.token);
  });

await program.parseAsync();
