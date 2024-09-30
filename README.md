<p align="center">
  <a href="https://www.powersync.com" target="_blank"><img src="https://github.com/powersync-ja/.github/assets/7372448/d2538c43-c1a0-4c47-9a76-41462dba484f"/></a>
</p>

*[PowerSync](https://www.powersync.com) is a sync engine for building local-first apps with instantly-responsive UI/UX and simplified state transfer. Syncs between SQLite on the client-side and Postgres or MongoDB on the server-side (MySQL coming soon).*

# PowerSync Service

`powersync-service` is the monorepo for the core [PowerSync Service](https://docs.powersync.com/architecture/powersync-service).

The service can be started using the public Docker image. See the image [notes](./service/README.md)

# Monorepo Structure:
## Packages

- [packages/service-core](./packages/service-core/README.md)

  - Core logic for the PowerSync backend server

- [packages/jpgwire](./packages/jpgwire/README.md)

  - Customized version of [pgwire](https://www.npmjs.com/package/pgwire?activeTab=dependencies) (used with Postgres)

- [packages/jsonbig](./packages/jsonbig/README.md)

  - Custom JSON and BigInt parser

- [packages/rsocket-router](./packages/rsocket-router/README.md)

  - Router for reactive streams using [RSocket](https://rsocket.io/)

- [packages/sync-rules](./packages/sync-rules/README.md)

  - Library containing logic for PowerSync sync rules

- [packages/types](./packages/types/)
  - Type definitions for the PowerSync Service

## Libraries

- [libs/lib-services](./libs/lib-services/README.md)

  - A light-weight set of definitions and utilities for micro services

## Service

- [service](./service/README.md)

Contains the PowerSync Service code. This project is used to build the `journeyapps/powersync-service` Docker image.

## Docs

- [docs](./docs/README.md)

Technical documentation regarding the implementation of PowerSync. 

## Test Client

- [test-client](./test-client/README.md)

Contains a minimal client demonstrating direct usage of the HTTP stream sync API. This can be used to test sync rules in contexts such as automated testing. 

# Developing

See the [notes](./DEVELOP.md) for local development instructions.
