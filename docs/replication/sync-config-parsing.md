# Sync config parsing and source-table options

`ServiceContext.syncConfigParser` is the shared parser for validation routes, deployments, replication, and loading
saved configs. Modules register additional parsers during initialization, before storage starts.

## Common source-table structure

Edition 3 sync configs declare source-specific options under `config.source_table_options`, alongside settings such as
`edition` and `storage_version`:

```yaml
config:
  edition: 3
  source_table_options:
    orders: {}
streams:
  orders:
    query: SELECT * FROM orders
```

Core owns the flat table-pattern map. Table keys follow the same right-to-left qualification as Sync Stream queries:
`table`, `database.table`, or `connection.database.table`. Omitted components remain relative to runtime defaults.
Declaring an entry does not add a replication source; SQL and stream definitions still select source tables.

Connection tags are accepted by the syntax and retained in parsed patterns and saved plans. During hydration, explicitly
qualified patterns currently use the `default` connection tag, even when another tag was authored. Do not rely on a
connection prefix to select a different replication connection. This preserves existing behavior; using connection tags
consistently across SQL, source-table options, and saved plans requires a future change covering all of those paths.

As in Sync Stream SQL, unquoted identifiers are converted to lowercase and double-quoted identifiers preserve their
authored case. Double quotes also allow dots without treating them as qualification separators. Since YAML removes its
own quotes, use an outer YAML quote to retain the identifier quotes, for example `'"audit.events"': {}`. The same
quoting applies to connection and database components. Inside a quoted identifier, two consecutive double quotes
represent one literal double quote, so `conn.test."na""me"` identifies the table `na"me`.

Keys that resolve to identical connection, database, and table components are rejected before module parsers run.
For example, `Users`, `users`, and `"users"` identify the same pattern, while `"Users"` remains distinct. Validation
names both conflicting keys and highlights the later declaration. Overlapping wildcard patterns remain allowed.
The same duplicate check applies when normalizing module-provided options and serializing or loading saved plans.

Core table options are empty and `additionalProperties: false` rejects options unless an external parser extends the
shared table-option schema. Multiple parsers may add independent fields to the same table.

Parsed, compiled, and hydrated representations expose `sourceTableConfig`. Modules inspect their own fields rather than
using a source-type discriminator. Authored table names and declaration order are persisted because wildcard precedence
may depend on both.

## Generic parser hooks

Register an `AdditionalSyncConfigParser` with `serviceContext.syncConfigParser.registerParser(...)`. Hooks are ordered
and identified by unique IDs:

- `extendJsonSchema({ schema })` can extend each entry in `config.source_table_options`. Preserve validation of unrelated
  fields and source modules. Additional root-level config fields are unsupported. Tooling reads a copy of the same
  schema through `syncConfigParser.jsonSchema`.
- `parse({ config, context })` receives decoded sync config. The context provides the candidate parsed config,
  SQL-selected source tables, default schema, source-location lookup, and diagnostic reporting. Merge owned values into
  `sourceTableConfig` without removing fields written by other parsers. For a `PrecompiledSyncConfig`, write the parser
  ID into `parsedConfig.plan.moduleData` with a `null` value when required.
- `validatePersisted({ config, context })` validates saved fields without reparsing SQL or relying on YAML locations.
  Modules with semantic restrictions beyond their JSON schema must repeat those checks here.

For example, a hook can report an option error using
`context.sourceLocations.getLocation(['config', 'source_table_options', table, 'option'])`. JSON Schema errors use the same
source locations. `patternErrorMessage` is accepted as an editor annotation while AJV still enforces `pattern`.

Hooks are synchronous and deterministic. Database connectivity, collection discovery, and index checks belong in source
validation. Some non-source parsing paths use a placeholder default schema, so modules must not persist names qualified
with that value.

Startup validation uses the replicator's default schema. MongoDB supplies the database from its normalized connection
config, allowing qualified source-table options to match unqualified SQL tables in that database before replication
starts. Source replicators with a known default schema should override `AbstractReplicator.defaultSchema`.

## Persistence and config deployment

Nonempty source-table options or required module IDs use sync-plan format 3 so an older service rejects options it
cannot interpret. Plans without either retain formats 1 and 2.

Required parser IDs are stored as keys of `moduleData`, with `null` values reserved for future module-owned data. Loading
a saved plan checks those IDs against registered parsers before running persisted validators and hydration. If a required
module is absent, loading fails. A failed saved plan is never replaced by silently reparsing its original source.

Source-table configurations must match before configs share incremental processing. Equality preserves table declaration
and expression order. Adding, changing, removing, or reordering source-table options requires replacement processing when
a new sync config is deployed; the active config can keep serving while that replacement is prepared.
