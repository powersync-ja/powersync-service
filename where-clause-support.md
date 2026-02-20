### WHERE Clause Support

Sync queries support a subset of standard SQL `WHERE` clause syntax. The allowed operators and combinations differ between Sync Streams and Sync Rules, and are more restrictive than standard SQL.

<Tabs>
  <Tab title="Sync Streams">

**`=` and `IS NULL`**

Compare a row column against a static value, a parameter, or another column:

```sql
-- Static value
WHERE
  status = 'active'
WHERE
  deleted_at IS NULL
  -- Parameter
WHERE
  owner_id = auth.user_id ()
WHERE
  region = connection.parameter ('region')
```

**`AND`**

Fully supported. Each condition is independent — you can mix parameter comparisons, subqueries, and row-value conditions freely in the same clause.

```sql
-- Two independent parameter conditions
WHERE
  owner_id = auth.user_id ()
  AND org_id = auth.parameter ('org_id')
  -- Parameter condition + row-value condition
WHERE
  owner_id = auth.user_id ()
  AND status = 'active'
  -- Parameter condition + subquery
WHERE
  list_id = subscription.parameter ('list_id')
  AND list_id IN (
    SELECT
      id
    FROM
      lists
    WHERE
      owner_id = auth.user_id ()
  )
```

**`OR`**

Supported, including `OR` nested inside `AND`. PowerSync automatically rewrites combinations like `A AND (B OR C)` into separate branches before evaluating.

```sql
-- Top-level OR
WHERE
  owner_id = auth.user_id ()
  OR shared_with = auth.user_id ()
  -- OR nested inside AND
WHERE
  status = 'active'
  AND (
    owner_id = auth.user_id ()
    OR shared_with = auth.user_id ()
  )
```

Each `OR` branch must be a valid filter on its own — you cannot have a branch that only makes sense in combination with the other branch.

**`NOT`**

Supported for simple conditions on row values:

```sql
WHERE
  status != 'archived'
WHERE
  deleted_at IS NOT NULL
WHERE
  category NOT IN ('draft', 'hidden')
```

Cannot negate an `IN` subquery or a parameter array expansion:

```sql
-- Not supported
WHERE
  issue_id NOT IN (
    SELECT
      id
    FROM
      issues
    WHERE
      owner_id = auth.user_id ()
  )
  -- Not supported
WHERE
  id NOT IN subscription.parameter ('excluded_ids')
```

<Note>`NOT IN` with a fixed value list works. `NOT IN` with a subquery or parameter does not.</Note>

  </Tab>
  <Tab title="Sync Rules (Legacy)">

**`=` and `IS NULL`**

Compare a row column against a static value or a bucket parameter:

```sql
-- Static value
WHERE
  status = 'active'
WHERE
  deleted_at IS NULL
  -- Bucket parameter
WHERE
  owner_id = bucket.user_id
```

**`AND`**

Supported in both parameter queries and data queries. In parameter queries, each condition may match a different parameter. However, you cannot combine two `IN` expressions on parameters in the same `AND` — split them into separate parameter queries instead.

```sql
-- Supported: parameter condition + row-value condition
WHERE
  users.id = request.user_id ()
  AND users.is_admin = TRUE
  -- Not supported: two IN expressions on parameters in the same AND
WHERE
  bucket.list_id IN lists.allowed_ids
  AND bucket.org_id IN lists.allowed_org_ids
```

**`OR`**

Supported in parameter queries only when both sides of the `OR` reference the exact same set of parameters. In practice this is rarely useful — use separate parameter queries instead.

```sql
-- Supported: both sides reference the same parameter
WHERE
  lists.owner_id = request.user_id ()
  OR lists.shared_with = request.user_id ()
  -- Not supported: sides reference different parameters
WHERE
  lists.owner_id = request.user_id ()
  OR lists.org_id = bucket.org_id
```

**`NOT`**

Supported for simple row-value conditions in data queries. Not supported on parameter-matching expressions in parameter queries.

```sql
-- Supported in data queries
WHERE
  status != 'archived'
WHERE
  deleted_at IS NOT NULL
WHERE
  category NOT IN ('draft', 'hidden')
  -- Not supported in parameter queries
WHERE
  NOT users.is_admin = TRUE
```
