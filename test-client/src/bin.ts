import { program } from 'commander';
import * as jose from 'jose';
import { getCredentials } from './auth.js';
import { getCheckpointData } from './client.js';
import { concurrentConnections } from './load-testing/load-test.js';

function loadEnvFile(path?: string) {
  if (path != null) {
    process.loadEnvFile(path);
  }
}

program
  .command('fetch-operations')
  .option('-t, --token [token]', 'JWT to use for authentication')
  .option('-e, --endpoint [endpoint]', 'endpoint URI')
  .option('-c, --config [config]', 'path to powersync.yaml, to auto-generate a token from a HS256 key')
  .option('-u, --sub [sub]', 'sub field for auto-generated token')
  .option('--env [env]', 'path to a .env file to load before resolving !env tags in the config')
  .option('--raw', 'output operations as received, without normalizing')
  .action(async (options) => {
    loadEnvFile(options.env);
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
  .option('--env [env]', 'path to a .env file to load before resolving !env tags in the config')
  .action(async (options) => {
    loadEnvFile(options.env);
    const credentials = await getCredentials(options);
    const decoded = await jose.decodeJwt(credentials.token);

    console.error(`Payload:\n${JSON.stringify(decoded, null, 2)}\nToken:`);
    console.log(credentials.token);
  });

program
  .command('concurrent-connections')
  .description('Load test the service by connecting a number of concurrent clients')
  .option('-t, --token [token]', 'JWT to use for authentication')
  .option('-e, --endpoint [endpoint]', 'endpoint URI')
  .option('-c, --config [config]', 'path to powersync.yaml, to auto-generate a token from a HS256 key')
  .option('-u, --sub [sub]', 'sub field for auto-generated token')
  .option('-n, --num-clients [num-clients]', 'number of clients to connect')
  .option('-m, --mode [mode]', 'http or websocket')
  .option('-p, --print [print]', 'print a field from the data being downloaded')
  .option('--env [env]', 'path to a .env file to load before resolving !env tags in the config')
  .option('--once', 'stop after the first checkpoint')
  .action(async (options) => {
    loadEnvFile(options.env);
    const credentials = await getCredentials(options);

    await concurrentConnections(
      {
        ...credentials,
        once: options.once ?? false,
        mode: options.mode
      },
      options['numClients'] ?? 10,
      options.print
    );
  });

await program.parseAsync();
