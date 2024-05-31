<p align="center">
  <a href="https://www.powersync.com" target="_blank"><img src="https://github.com/powersync-ja/.github/assets/19345049/602bafa0-41ce-4cee-a432-56848c278722"/></a>
</p>

_Bad connectivity is everywhere, and we're tired of it. PowerSync is on a mission to help developers write offline-first real-time reactive apps._

[PowerSync](https://powersync.com) is a service and set of SDKs that keeps Postgres databases in sync with on-device SQLite databases.

# PowerSync Service

`powersync-service` is the monorepo for the core PowerSync service.

## Monorepo Structure: Packages

- [packages/service-core](./packages/service-core/README.md)

  - Core logic for the PowerSync backend server

- [packages/jpgwire](./packages/jpgwire/README.md)

  - Customized version of [pgwire](https://www.npmjs.com/package/pgwire?activeTab=dependencies)

- [packages/jsonbig](./packages/jsonbig/README.md)

  - Custom JSON and BigInt parser

- [packages/rsocket-router](./packages/rsocker-router/README.md)

  - Router for reactive streams using [RSocket](https://rsocket.io/)

- [packages/sync-rules](./packages/sync-rules/README.md)

  - Library containing logic for PowerSync sync rules

- [packages/types](./packages/types/README.md)
  - Type definitions for the PowerSync service

## Service

The PowerSync service code is located in the `service` folder. This project is used to build the `journeyapps/powersync-service` Docker image.

# Notes

This mono repo currently relies on `restricted` packages. Currently this repo can only be built in CI. These dependencies will be removed soon.

The service can be started using the public Docker image. See image [notes](./service/README.md)
