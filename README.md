<p align="center">
  <a href="https://www.powersync.com" target="_blank"><img src="https://github.com/powersync-ja/.github/assets/7372448/d2538c43-c1a0-4c47-9a76-41462dba484f"/></a>
</p>

*[PowerSync](https://www.powersync.com) is a Postgres-SQLite sync layer, which helps developers to create local-first real-time reactive apps that work seamlessly both online and offline.*

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
