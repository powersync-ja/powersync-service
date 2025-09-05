# Test Client

This is a minimal client demonstrating direct usage of the HTTP stream sync API.

For a full implementation, see our client SDKs.

## Setup
1. Install dependencies on the project root
```shell
# In project root directory
pnpm install
pnpm build:packages
```
2. Build the test-client in the `test-client` directory
```shell
# In the test-client directory
pnpm build
```

## Usage

### fetch-operations
The `fetch-operations` command downloads data for a single checkpoint, and outputs a normalized form: one CLEAR operation, followed by the latest PUT operation for each row. This normalized form is still split per bucket. The output is not affected by compacting, but can be affected by replication order.

To avoid normalizing the data, use the `--raw` option. This may include additional CLEAR, MOVE, REMOVE and duplicate PUT operations.

```sh
# If the endpoint is not available in the token aud field, add the --endpoint argument
node dist/bin.js fetch-operations --token <token> --endpoint http://localhost:8080

# If the endpoint is present in token aud field, it can be omitted from args:
node dist/bin.js fetch-operations --token <token>

# If a local powersync.yaml is present with a configured HS256 key, this can be used:
node dist/bin.js fetch-operations --config path/to/powersync.yaml --endpoint http://localhost:8080

# Without endpoint, it defaults to http://127.0.0.1:<port> from the config:
node dist/bin.js fetch-operations --config path/to/powersync.yaml

# Use --sub to specify a user id in the generated token:
node dist/bin.js fetch-operations --config path/to/powersync.yaml --sub test-user
```

### generate-token

Used to generate a JWT token based on your current PowerSync YAML config.

```sh
node dist/bin.js generate-token --config path/to/powersync.yaml --sub test-user
```

### concurrent-connections

Use this command to simulate concurrent connections to a PowerSync instance. This can be used for performance benchmarking
and other load-testing use cases. There are two modes available, `websocket` or `http`. By default, the command uses the
`http` mode.

```shell
# Send two concurrent requests to request a download of sync operations using -n to specify the number of connections
node ./dist/bin.js concurrent-connections -n 2 -t <token> 

# Send two concurrent requests to request a download of sync operations using websocket mode
node ./dist/bin.js concurrent-connections -n 2 -t <token> -m websocket
```

Once the sync has completed for a connection the command will print the `op_id`, `ops`, `bytes`, `duration` and `data` for
each connection. By default, the `data` value will be an array id's of the rows synced from the source database, but you can
specify an additional argument if you want to print a specific field from the data being synced.

```shell
# Send two concurrent requests and print the name field, as an example.
node ./dist/bin.js concurrent-connections -n 2 -t <token> -p name
```

