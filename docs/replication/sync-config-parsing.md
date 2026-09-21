# Sync config parsing and connection options

`ServiceContext.syncConfigParser` is the shared parser for validation routes, deployments, replication, and loading
saved configs. Modules register additional parsers during initialization, before storage starts.

## Common connection structure

Edition 3 sync configs declare connection options under `config.connections`, alongside settings such as `edition`
and `storage_version`:

```yaml
config:
  edition: 3
  connections:
    default:
      type: mongodb
      tables:
        orders: {}
streams:
  orders:
    query: SELECT * FROM orders
```

This repo defines the connection-tag map, the `type` discriminator, and optional table-name/pattern map. Its table options are
empty: both `additionalProperties: false` and `maxProperties: 0` explicitly reject options. Actual options require an external
module to allow them.

An empty connection `{}` is normalized away. A nonempty connection requires `type` and cannot contain unknown
connection properties. The connection options have no version field.

The parsed, compiled, and hydrated representations expose `connectionConfig`. The common `ConnectionConfig<TTableConfig>`
type and `connectionConfigCodec(tableCodec)` let modules specialize table options without importing module types into
core. Modules inspect `type` before using their options. Unqualified names remain relative to the source connection's default schema.

## Generic parser hooks

Register an `AdditionalSyncConfigParser` with `serviceContext.syncConfigParser.registerParser(...)`. Hooks are ordered
and identified by unique IDs, independently of connection types:

- `extendJsonSchema({ schema })` can specialize the full resolved JSON schema. Preserve validation of unrelated fields
  and connection types. Additional root-level config fields are not supported by the parser, even if declared in the schema.
  Tooling reads a copy of the same schema through `syncConfigParser.jsonSchema`.
- `parse({ config, context })` receives decoded sync config. The context provides the candidate parsed config, SQL-selected
  source tables, default schema, source-location lookup, and diagnostic reporting. Store additional options on the appropriate
  config fields, such as `connectionConfig`. Modules own conversion of their input into parsed state; core does not copy
  connection options from the decoded input. Add the parser ID to `parsedConfig.additionalModuleIds` when used. A fatal diagnostic rejects the candidate, including in diagnostic-only parsing mode.
- `validatePersisted({ config, context })` validates saved config fields without reparsing SQL or relying on config
  source locations. Modules with semantic restrictions beyond their JSON schema must repeat those checks here.

For example, a hook can report an error at a specific table option using
`context.sourceLocations.getLocation(['config', 'connections', tag, 'tables', table, 'option'])`. JSON schema errors use the
same config source locations, including escaped JSON-pointer segments. `patternErrorMessage` is accepted as an editor annotation;
AJV still enforces `pattern` and rejects unknown schema keywords.

Hooks are synchronous and deterministic. Database connectivity, collection discovery, and index checks belong in source
validation. Some non-source parsing paths use a placeholder default schema, so do not persist qualified names derived
from that context; resolve relative connection table names when the source connection is available.

## Persistence and config deployment

Nonempty connection options or required module IDs use sync-plan format 3 so an older service rejects options it cannot interpret. Plans without either
retain formats 1 and 2.

Loading a saved plan checks its persisted `additionalModuleIds` against registered parser IDs before running the required
modules' persisted validators and hydrating it. If a required module is absent, loading fails. The authoring JSON schema
is not applied to the persisted representation. Plans without module IDs have no declared module dependencies. A failed saved plan is never replaced by
silently re-parsing the original config source.

Connection maps must match before configs share incremental processing. The service checks persisted configs when
assembling an incremental replication stream and throws a `ReplicationAssertionError` on a mismatch, before hydration.
Equality normalizes
connection-tag order but preserves table and expression order. Adding, changing, or removing connection options requires
replacement processing when a new sync config is deployed; the active config can keep serving while that replacement
is prepared. This comparison is deliberately conservative and has no fingerprints or module compatibility callbacks.
