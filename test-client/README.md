# Test Client

This is a minimal client demonstrating direct usage of the HTTP stream sync api.

For a full implementation, see our client SDKs.

## Usage

```sh
# In project root
pnpm install
pnpm build:packages
# In this folder
pnpm build
node dist/bin.js fetch-operations --token <token> --endpoint http://localhost:8080

# More examples:

# If the endpoint is present in token aud field, it can be omitted from args:
node dist/bin.js fetch-operations --token <token>

# If a local powersync.yaml is present with a configured HS256 key, this can be used:
node dist/bin.js fetch-operations --config path/to/powersync.yaml --endpoint http://localhost:8080

# Without endpoint, it defaults to http://127.0.0.1:<port> from the config:
node dist/bin.js fetch-operations --config path/to/powersync.yaml

# Use --sub to specify a user id in the generated token:
node dist/bin.js fetch-operations --config path/to/powersync.yaml --sub test-user
```

The script will normalize the data in each bucket to a single CLEAR operation, followed by the latest PUT operation for each row.

To get the raw operations instead, which may additionally include CLEAR, MOVE, REMOVE and duplicate PUT operations, use the `--raw` flag.
