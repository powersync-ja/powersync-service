# powersync-sync-rules

A library containing logic for PowerSync sync rules.

This is not intended to be used directly by users of PowerSync. If you are interested in the internals, read on.

# Overview

A core design constraint is that sync rules define two operations:

1. Given a data row, compute a list of buckets that it belongs to.
2. Given an authenticated user, return a list of buckets for the user.

This implementation of sync rules use SQL queries to declaratively define those operations using familiar SQL operations.

We define (1) using data queries, and (2) using parameter queries.

Example:

```yaml
bucket_definitions:
  by_org:
    # parameter query
    # This defines bucket parameters are `bucket.org_id`
    parameters: select org_id from users where id = token_parameters.user_id
    # data query
    data:
      - select * from documents where org_id = bucket.org_id
```

For the above example, a document with `org_id: 'org1'` will belong to a single bucket `by_org["org1"]`. Similarly, a user with `org_id: 'org1'` will sync the bucket `by_org["org1"]`.

An important aspect is that none of these SQL queries are actually executed against any SQL database. Instead, it is used to pre-process data before storing the data in a format for efficient sync operations.

When data is replicated from the source database to PowerSync, we do two things for each row:

1. Evaluate data queries on the row: `syncRules.evaluateRow(row)`.
2. Evaluate parameter queries on the row: `syncRules.evaluateParameterRow(row)`.

Data queries also have the option to transform the row instead of just using `select *`. We store the transformed data for each of the buckets it belongs to.

# Query Structure

## Data queries

A data query is turned into a function `(row) => Array<{bucket, data}>`. The main implementation is in the `SqlDataQuery` class.

The main clauses in a data query are the ones comparing bucket parameters, for example `WHERE documents.document_org_id = bucket.bucket_org_id`. In this case, a document with `document_org_id: 'org1'` will have a bucket parameter of `bucket_org_id: 'org1'`.

A data query must match each bucket parameter. To be able to always compute the bucket ids, there are major limitations on the operators supported with bucket parameters, as well as how expressions can be combined using AND and OR.

The WHERE clause of a data query is compiled into a `ParameterMatchClause`.

Query clauses are structured as follows:

```SQL
'literal' -- StaticValueClause
mytable.column -- RowValueClause
fn(mytable.column) -- RowValueClause. This includes most operators.
bucket.param -- ParameterValueClause
fn(bucket.param) -- Error: not allowed

mytable.column = mytable.other_column -- RowValueClause
mytable.column = bucket.param -- ParameterMatchClause
bucket.param IN mytable.some_array -- ParameterMatchClause
(mytable.column1 = bucket.param1) AND (mytable.column2 = bucket.param2) -- ParameterMatchClause
(mytable.column1 = bucket.param) OR (mytable.column2 = bucket.param) -- ParameterMatchClause
```

## Parameter Queries

There are two types of parameter queries:

1. Queries without tables. These just operate on request parameters. Example: `select token_parameters.user_id`. Thes are implemented in the `StaticSqlParameterQuery` class.
2. Queries with tables. Example: `select org_id from users where id = token_parameters.user_id`. These use parameter tables, and are implemented in `SqlParameterQuery`. These are used to pre-process rows in the parameter tables for efficient lookup later.

### StaticSqlParameterQuery

These are effecitively just a function of `(request) => Array[{bucket}]`. These queries can select values from request parameters, and apply filters from request parameters.

The WHERE filter is a ParameterMatchClause that operates on the request parameters.
The bucket parameters are each a RowValueClause that operates on the request parameters.

Compiled expression clauses are structured as follows:

```SQL
'literal' -- StaticValueClause
token_parameters.param -- RowValueClause
fn(token_parameters.param) -- RowValueClause. This includes most operators.
```

The implementation may be refactored to be more consistent with `SqlParameterQuery` in the future - using `RowValueClause` for request parameters is not ideal.

### SqlParameterQuery

These queries pre-process parameter tables to effectively create an "index" for efficient queries when syncing.

For a parameter query `select org_id from users where users.org_id = token_parameters.org_id and lower(users.email) = token_parameters.email`, this would effectively create an index on `users.org_id, lower(users.email)`. These indexes are referred to as "lookup" values. Only direct equality lookups are supported on these indexes currently (including the IN operator). Support for more general queries such as "greater than" operators may be added later.

A SqlParameterQuery defines the following operations:

1. `evaluateParameterRow(row)`: Given a parameter row, compute the lookup index entries.
2. `getLookups(request)`: Given request parameters, compute the lookup index entries we need to find.
3. `queryBucketIds(request)`: Uses `getLookups(request)`, combined with a database lookup, to compute bucket ids from request parameters.

The compiled query is based on the following:

1. WHERE clause compiled into a `ParameterMatchClause`. This computes the lookup index.
2. `lookup_extractors`: Set of `RowValueClause`. Each of these represent a SELECT clause based on a row value, e.g. `SELECT users.org_id`. These are evaluated during the `evaluateParameterRow` call.
3. `static_extractors`. Set of `RowValueClause`. Each of these represent a SELECT clause based on a request parameter, e.g. `SELECT token_parameters.user_id`. These are evaluated during the `queryBucketIds` call.

Compiled expression clauses are structured as follows:

```SQL
'literal' -- StaticValueClause
mytable.column -- RowValueClause
fn(mytable.column) -- RowValueClause. This includes most operators.
token_parameters.param -- ParameterValueClause
fn(token_parameters.param) -- ParameterValueClause
fn(mytable.column, token_parameters.param) -- Error: not allowed

mytable.column = mytable.other_column -- RowValueClause
mytable.column = token_parameters.param -- ParameterMatchClause
token_parameters.param IN mytable.some_array -- ParameterMatchClause
mytable.some_value IN token_parameters.some_array -- ParameterMatchClause

(mytable.column1 = token_parameters.param1) AND (mytable.column2 = token_parameters.param2) -- ParameterMatchClause
(mytable.column1 = token_parameters.param) OR (mytable.column2 = token_parameters.param) -- ParameterMatchClause
```
