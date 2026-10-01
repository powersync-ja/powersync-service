# Sync config parsing and source-table options

`ServiceContext.syncConfigParser` is the shared parser for validation routes, deployments, replication, and loading
saved configs. Modules register additional parsers during initialization, before storage starts.

## Common source-table structure

Edition 3 sync configs declare source-specific options under `config.source_tables`, alongside settings such as
`edition` and `storage_version`:

```yaml
config:
  edition: 3
  source_tables:
    orders: {}
streams:
  orders:
    query: SELECT * FROM orders
```

Core owns the flat table-pattern map. Table keys follow the same right-to-left qualification as Sync Stream queries:
`table`, `database.table`, or `connection.database.table`. Omitted components remain relative to runtime defaults.
Declaring an entry does not add a replication source; SQL and stream definitions still select source tables.

Core table options are empty and `additionalProperties: false` rejects options unless an external parser extends the
shared table-option schema. Multiple parsers may add independent fields to the same table.

Parsed, compiled, and hydrated representations expose `sourceTableConfig`. Modules inspect their own fields rather than
using a source-type discriminator. Authored table names and declaration order are persisted because wildcard precedence
may depend on both.

## Generic parser hooks

Register an `AdditionalSyncConfigParser` with `serviceContext.syncConfigParser.registerParser(...)`. Hooks are ordered
and identified by unique IDs:

- `extendJsonSchema({ schema })` can extend each entry in `config.source_tables`. Preserve validation of unrelated
  fields and source modules. Additional root-level config fields are unsupported. Tooling reads a copy of the same
  schema through `syncConfigParser.jsonSchema`.
- `parse({ config, context })` receives decoded sync config. The context provides the candidate parsed config,
  SQL-selected source tables, default schema, source-location lookup, and diagnostic reporting. Merge owned values into
  `sourceTableConfig` without removing fields written by other parsers. For a `PrecompiledSyncConfig`, write the parser
  ID into `parsedConfig.plan.moduleData` with a `null` value when required.
- `validatePersisted({ config, context })` validates saved fields without reparsing SQL or relying on YAML locations.
  Modules with semantic restrictions beyond their JSON schema must repeat those checks here.

For example, a hook can report an option error using
`context.sourceLocations.getLocation(['config', 'source_tables', table, 'option'])`. JSON Schema errors use the same
source locations. `patternErrorMessage` is accepted as an editor annotation while AJV still enforces `pattern`.

Hooks are synchronous and deterministic. Database connectivity, collection discovery, and index checks belong in source
validation. Some non-source parsing paths use a placeholder default schema, so modules must not persist names qualified
with that value.

## Persistence and config deployment

Nonempty source-table options or required module IDs use sync-plan format 3 so an older service rejects options it
cannot interpret. Plans without either retain formats 1 and 2.

Required parser IDs are stored as keys of `moduleData`, with `null` values reserved for future module-owned data. Loading
a saved plan checks those IDs against registered parsers before running persisted validators and hydration. If a required
module is absent, loading fails. A failed saved plan is never replaced by silently reparsing its original source.

Source-table configurations must match before configs share incremental processing. Equality preserves table declaration
and expression order. Adding, changing, removing, or reordering source-table options requires replacement processing when
a new sync config is deployed; the active config can keep serving while that replacement is prepared.
