# powersync-jsonbig

JSON is used everywhere, including:
1. PostgreSQL (json/jsonb types)
2. Sync rules input (values are normalized to JSON text).
3. Sync rule transformations (extracting values, constructing objects in the future)
4. Persisting data in the database.
5. Sending to the client.

Where we can, JSON data is kept as strings and not parsed.
This is so that:
1. We don't add parsing / serializing overhead.
2. We don't change the data.

Specifically:
1. The SQLite type system makes a distinction between INTEGER and REAL values. We try to preserve this.
2. Integers in SQLite can be up to 64-bit.

For this, we use a custom parser, and use BigInt for all integers, and number for floats.
