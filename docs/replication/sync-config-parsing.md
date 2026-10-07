# Source-table options

Sync configs can specify source-specific replication options under `config.source_table_options`. This is a map from
table names or patterns to option objects. The currently supported option is `mongodb_filter_expression`, which
specifies MongoDB replication pre-filtering:

```yaml
config:
  edition: 3
  source_table_options:
    orders:
      mongodb_filter_expression:
        $eq: ['$$doc.active', true]
streams:
  orders:
    query: SELECT * FROM orders
```

Executing MongoDB replication pre-filtering requires additional external modules. The service parses and validates
these expressions, but rejects their deployment when the configured source adapter does not support the feature.

## Table names and patterns

Table keys use the same right-to-left qualification as Sync Stream queries: `table`, `database.table`, or
`connection.database.table`. Omitted components use runtime defaults. Declaring source-table options does not add
a replication source; the stream queries still select the source tables.

Connection prefixes are accepted and retained in saved configuration. However, query hydration currently uses the
`default` connection tag for explicitly qualified patterns. A connection prefix should not be used to select a
different replication connection.

Unquoted identifiers are converted to lowercase. Double-quoted identifiers preserve case and may contain dots.
Use an outer YAML quote to retain the identifier quotes, for example `'"audit.events"': {}`. Inside a quoted
identifier, two double quotes represent one literal double quote.

Keys that resolve to the same connection, database, and table pattern are rejected. For example, `Users`, `users`,
and `"users"` identify the same pattern, while `"Users"` remains distinct. Overlapping wildcard patterns are allowed.

## MongoDB pre-filtering expressions

`mongodb_filter_expression` accepts an expression or the string `disabled`. Expressions support:

- `$eq`: a source field and a literal value, such as `{ $eq: ['$$doc.active', true] }`.
- `$in`: a source field and a nonempty list of literal values.
- `$and` and `$or`: nonempty lists of nested filter expressions.

Source fields use the `$$doc.` prefix. Literal values include strings, numbers, booleans, and supported Extended JSON
wrappers for BSON values. Unknown source-table options and invalid expressions are rejected during parsing.

The sync-config JSON Schema includes expression shapes and examples for editor autocomplete. Runtime validation
also checks BSON literal values and reports errors at the relevant YAML keys or values. Source capability checks
run during validation, deployment, and reprocessing; validation can report capability failures alongside other
diagnostics. File-loaded sync configs are checked after parsing at replication startup. With `exit_on_error`
enabled, source-capability errors fail startup before persistence. With it disabled, failures are logged and
configs are persisted for diagnostics. The shared `AbstractReplicationStream` checks persisted configs for fatal parsing errors and source capabilities
before snapshotting or streaming. Warnings do not block replication. Failures block replication and are reported through storage; the job releases its lock and validation
is retried when a new job starts.

## Saved configuration

Compiled plans retain the options in `sourceTableConfig`. Hydrated configs expose the options from the first underlying
plan; configs sharing a replication reader must have identical options. Saved plans preserve authored
table names and declaration order. Source-table options use sync-plan format 3; plans without them retain formats
1 and 2. Loading a saved plan validates its source-table options without reparsing SQL.

Changing source-table options requires replacement processing when deploying a new sync config.

Source adapters return `ValidationDiagnostic[]` from `validateSourceCapabilities`. Each diagnostic has a `level`
(`warning` or `fatal`), a `message`, and optional source location and timestamp. Deployment and reprocessing reject
fatal diagnostics; warnings are advisory. Validation collects diagnostics and continues independent table checks.
Unexpected adapter failures may still throw and are reported as fatal errors. `ReplicationError` remains an alias
for the same diagnostic shape used in existing response fields.
