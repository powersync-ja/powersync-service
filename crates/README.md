This directory contains Rust sources used in the PowerSync Service.

The crates used here are:

1. `powersync_service`: Rust functionality we use in the service, as a pure-Rust library without depending on Node.
2. `./js_addon` (`powersync_service_js_addon_shared`): Exposes functionality from `powersync_service` using Node addon APIs.
3. `open_edition_js_entrypoint`: The actual entrypoint being compiled into a shared library for Node. We keep this separate because PowerSync Cloud and Enterprise editions can add additional Rust code, and thus need another entrypoint.

Because all Rust code is compiled into a single Node addon (instead of being distributed as independent modules), we keep most functionality in a single crate instead of defining one per module.

## Calling Rust

The entrypoint continues to be written in TypeScript, functionality written in
Rust is consumed through the existing service container pattern. We want to
avoid compiling multiple Rust entrypoints, so there is a single Rust module
responsible for registering the services it provides.

### Modularity

Rust doesn't have a stable ABI, so we can't load modules as multiple dynamic
libraries without duplicating compiled code. Instead, all Rust helpers are
compiled into a single Node addon loaded by the service.

However, we still want Rust code to be written in a modular way. PowerSync
Cloud also contains internal changes offering additional features for paid
plans. These private builds re-use the public `powersync_service` and
`powersync_service_js_addon_shared` crates, but add their own entrypoint and
a custom JavaScript `RustModule` loader to inject additional functionality.

## Testing

Most of our Rust code is an optimization for existing functionality implemented
in TypeScript. This makes it attractive to test Rust code via JavaScript
interfaces and `vitest`. Unlike cargo unit tests, this also covers `napi`
bindings.

As we move more code to Rust, we might want to rely on `cargo test` in the
future.

## Debugging

### Experimental MongoDB row converter

`RustSourceRowConverter` from `@powersync/service-module-mongodb` implements the
same interface as `DirectSourceRowConverter`. It is opt-in; the replication
paths still use the TypeScript converter. Create it with a compatibility context
and the configured `RustModule`:

```ts
const converter = new RustSourceRowConverter(compatibilityContext, rustModule);
const { row, replicaId } = converter.rawToSqliteRow(bsonBuffer);
```

Rust parses the BSON, renders nested JSON, and returns SQLite values through
N-API. It extracts the first `_id` element during that pass and returns it as a
small BSON document, which the Node BSON library decodes to preserve replica
identity types. Date precision and nested boolean formatting follow the supplied
compatibility context. Like the direct converter, it preserves raw regex flags,
uses replacement characters for invalid UTF-8, and omits nested binary values
except UUIDs. Malformed BSON is rejected, but exact error messages can differ.

To compare performance, build the TypeScript packages and a release addon:

```sh
pnpm exec tsc -b modules/module-mongodb
pnpm rust:build-addon --release
POWERSYNC_RUST_ADDON=/absolute/path/to/open_edition_js_entrypoint.node \
  node modules/module-mongodb/scripts/benchmark-change-document-json.mts
```

The build command prints the addon path. BSON conversion parity tests in
`modules/module-mongodb/test/src/buffer_to_sqlite.test.ts` also exercise this
implementation, using `POWERSYNC_RUST_ADDON` or building a debug addon automatically.

The benchmark defaults to ASCII text in a top-level field. Use `--payload unicode`,
`--payload nested`, or `--payload objects` to compare other workloads, and
`--rows-only` to exclude change-event parsing. `--iterations 1000` overrides the
automatically chosen iteration count for quick profiling runs. For example:

```sh
POWERSYNC_RUST_ADDON=/absolute/path/to/open_edition_js_entrypoint.node \
  node modules/module-mongodb/scripts/benchmark-change-document-json.mts --rows-only --payload objects
cargo run --release -p powersync_service --example mongo_conversion
```

The Rust example measures the parser without N-API or JavaScript conversion.
Keep those measurements separate: copying strings into V8 and setting row
properties can cost more than parsing BSON. The converter borrows valid BSON
strings and binary during the synchronous call, writes nested JSON into one
buffer, and uses ASCII/UTF-16 string constructors and scalar BigInt constructors
at the N-API boundary. Returned JavaScript values own their data.

The experimental converter uses `node_api_create_object_with_properties` from
the repository's pinned Node version to construct rows in one operation. It
retains optimized property keys for one row shape, bounded to 256 columns and
16 KiB of key text. Two consecutive occurrences warm a new shape; changing
documents use property descriptors until then. The reference holding the
keys is released when the shape is replaced or the converter is collected.
Oversized shapes and duplicate fields also use property descriptors. Both paths
define own data properties, so fields named `__proto__` retain their values
without changing the row's prototype.

### Attaching a debugger

You can debug Rust code loaded as a Node addon via LLDB. Run `pnpm -w rust:build-addon`
for a debug build of the addon, then launch the service with a `--rust-addon <path>`
towards the emitted `.node` file.

After launching the service, attach LLDB to the running Node process. After the
Rust addon was loaded, you can set breakpoints in Rust sources.

In VS Code, the [CodeLLDB extension](https://marketplace.visualstudio.com/items?itemName=vadimcn.vscode-lldb)
may be useful for this.

## Publishing

We don't publish compiled binaries to NPM. Instead:

- Containers build the node addon for their target platform.
- When running the service locally, run `pnpm -w rust:build-addon` (it prints the output path) and launch the service with `--rust-addon <path>`.
