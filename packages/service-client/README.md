# @powersync/service-client

TypeScript client for the PowerSync instance admin API. Use it to run SQL, fetch diagnostics, manage schema, validate sync rules, and reprocess sync state.

## Installation

```bash
pnpm add @powersync/service-client @journeyapps-labs/common-sdk @powersync/service-types
```

## Configuration

The Authorization token must be one of the tokens defined in your PowerSync config:

```yaml
api:
  tokens:
    - use_a_better_token_in_production
```

## Usage

```typescript
import * as sdk from '@journeyapps-labs/common-sdk';
import { InstanceClient } from '@powersync/service-client';

const client = new InstanceClient({
  client: sdk.createNodeNetworkClient({
    headers: () => ({
      Authorization: 'Bearer <token-from-config>'
    })
  }),
  endpoint: 'https://[Your PowerSync URL here]'
});

// Execute SQL, get diagnostics, schema, validate sync rules, or reprocess
const result = await client.executeSql({ sql: { query: 'SELECT 1', args: [] } });
const diag = await client.diagnostics({ sync_rules_content: true });
const schema = await client.getSchema({});
const validation = await client.validate({ sync_rules: '...' });
const resp = await client.reprocess({});
```

## API

| Method        | Description                                      |
| ------------- | ------------------------------------------------ |
| `executeSql`  | Run a SQL query on the instance                  |
| `diagnostics` | Get connections and sync rules health            |
| `getSchema`  | Get the current schema (connections, tables)      |
| `validate`   | Validate sync rules against the current schema   |
| `reprocess`  | Reprocess sync state for the instance            |

Request and response types are exported from `@powersync/service-types` (internal routes). See the `InstanceClient` and method JSDoc in source for examples and shapes.
