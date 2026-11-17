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
