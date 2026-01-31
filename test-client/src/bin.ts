import { program } from 'commander';
import { getCheckpointData } from './client.js';
import { getCredentials } from './auth.js';
import * as jose from 'jose';
import { concurrentConnections } from './load-testing/load-test.js';

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
  .description('Generate a JWT from a given powersync.yaml config file')
  .option('-c, --config [config]', 'path to powersync.yaml')
  .option('-u, --sub [sub]', 'payload sub')
  .option('-e, --endpoint [endpoint]', 'additional payload aud (legacy)')
  .option('--aud <audience...>', 'JWT audience (repeatable)')
  .option('--exp <duration>', 'JWT expiration time (e.g. 15m, 1h, 24h)')
  .action(async (options) => {
    const credentials = await getCredentials(options);
    const decoded = jose.decodeJwt(credentials.token);

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
  .option('--once', 'stop after the first checkpoint')
  .action(async (options) => {
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
