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

The PowerSync Service requires Postgres and MongoDB server connections. These configuration details can be specified in a `powersync.yaml` (or JSON) configuration file.

See the [self-hosting demo](https://github.com/powersync-ja/self-host-demo) for demos of starting these services.

A quick method for running all required services with a handy backend and frontend is to run the following in a checked-out `self-host-demo` folder.

```bash
docker compose -f demos/nodejs/docker-compose.yaml up --scale powersync=0
```

Note: The `mongo` hostname specified in the MongoDB replica set needs to be accessible by your host machine if using the Mongo service above.

One method to obtain access is to add the following to `/etc/hosts` (on Unix-like operating systems)

```
127.0.0.1 mongo
```

This will start all the services defined in the self-hosting demo except for the PowerSync Service - which will be started from this repository.

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
export MONGO_TEST_URL="mongodb://localhost:27017/powersync_test"
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

# Formatting

[prettier](https://prettier.io/) is used to automatically format most source files. To ensure all contributions have consistent formatting, use one or more of these options:

1. Configure your editor to use prettier on save.
2. Use `pnpm format:dirty` to format changes before committing.
3. Use `pnpm configure-hooks` to install pre-commit hooks, that validate the formatting when committing.

# Releases

This repository uses Changesets. Add changesets to changed packages before merging PRs.

```bash
changeset add
```

Merging a PR with changeset files will automatically create a release PR. Merging the release PR will bump versions, tag and publish packages.

The Docker image is published by manually triggering the `Docker Image Release` Github Action. The Docker image version is extracted from the `./service/package.json` `version` field.
