import { program } from 'commander';
import { getCheckpointData } from './client.js';

program
  .command('fetch-operations')
  .option('-t, --token [token]')
  .option('-e, --endpoint [endpoint]')
  .option('--raw')
  .action(async (options) => {
    const data = await getCheckpointData({ endpoint: options.endpoint, token: options.token, raw: options.raw });
    console.log(JSON.stringify(data, null, 2));
  });

await program.parseAsync();
