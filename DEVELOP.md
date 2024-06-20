# Developing Instructions

# Getting Started

This repository uses PNPM. Install packages

```bash
pnpm install
```

The project uses TypeScript. Build packages with

```bash
pnpm build
```

# Running Service

## Dependent Services

The PowerSync service requires Postgres and MongoDB server connections. These configuration details can be specified in a `powersync.yaml` (or JSON) configuration file.

See the [Self hosting demo](https://github.com/powersync-ja/self-host-demo) for examples of starting these services.

A quick method for running all required services with a handy backend and frontend is to run the following in a checked-out `self-host-demo` folder.

```bash
docker compose up --scale powersync=0
```

This will start all the services defined in the Self hosting demo except for the PowerSync service - which will be started from this repository.

## Local Configuration

The `./service` folder contains a NodeJS project which starts all PowerSync service operations.

Copy the template configuration files and configure any changes to your local needs.

```bash
cd ./service
cp local-dev/powersync-template.yaml powersync.yaml
cp local-dev/sync-rules-template.yaml sync-rules.yaml
```

## Starting Service

The service can be started with watching changes to any consumed source files by running the `pnpm watch:service` command in the repository root.

# Running Tests

Most packages should contain a `test` script which can be executed with `pnpm test`. Some packages may require additional setup to run tests.

## Service Core

Some tests for these packages require a connection to MongoDB and Postgres. Connection strings for these services should be available as environment variables. See [Running Tests](#running-services) for details on configuring those services.

These can be set in a terminal/shell

```bash
export MONGO_TEST_UR="mongodb://localhost:27017/powersync_test"
export PG_TEST_URL="postgres://postgres:postgres@localhost:5432/powersync_test"
```

or by copying the `.env.template` file and using a loader such as [Direnv](https://direnv.net/)

```bash
cp .env.template .env
```

## Postgres Configuration

The default `PG_TEST_URL` points to a `powersync_test` database. Ensure this is created by executing the following SQL on your connection.

```SQL
CREATE DATABASE powersync_test;
```

# Releases

This repository uses Changesets. Add changesets to changed packages before merging PRs.

```bash
changeset add
```

Merging a PR with changeset files will automatically create a release PR. Merging the release PR will bump versions, tag and publish packages and the Docker image. The Docker image version is extracted from the `./service/package.json` `version` field.
