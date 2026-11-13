# jpgwire

pgwire is a nice low-level lib that supports postgres streaming replication.

Unfortunately, it is not very mature, and we need some modifications.

This package includes the base lib, which it then extends and customizes

Additionally, this includes wrappers to connect using PowerSync config, as well as a set of standard CA certificates.

## pgwire

Originally authored by [exedealer](https://github.com/exedealer/pgwire) under the MIT license.

## Upstream Node tests

The tests in `test/upstream` are copied from [pgwire](https://github.com/exedealer/pgwire)
(version 0.8.3), with imports pointing at this package's vendored `src/pgwire/index.js`.

Run the suite with Docker running:

```sh
pnpm --filter @powersync/service-jpgwire test:upstream
```

Set `TEST_FILTER` to a Node test name pattern to focus a run:

```sh
TEST_FILTER='auth' pnpm --filter @powersync/service-jpgwire test:upstream
```

Remove the test containers and socket volume after running:

```sh
pnpm --filter @powersync/service-jpgwire test:upstream:down
```
