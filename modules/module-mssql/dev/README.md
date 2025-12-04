# MSSQL Dev Database

This directory contains Docker Compose configuration for running a local MSSQL Server instance with CDC (Change Data Capture) enabled for development and testing. The image used is the 2022 Edition of SQL Server. 2025 can also be used, but has issues on Mac OS X 26 Tahoe due to this issue: https://github.com/microsoft/mssql-docker/issues/942

## Prerequisites

- Docker and Docker Compose installed
- A `.env` file in this directory see the `.env.template` for required variables

## Environment Variables

```bash
ROOT_PASSWORD=
DATABASE=
DB_USER=
DB_USER_PASSWORD=
```

**Note:** The `ROOT_PASSWORD` and `DB_USER_PASSWORD` must meet SQL Server password complexity requirements (at least 8 characters, including uppercase, lowercase, numbers, and special characters).

## Usage

### Starting the Database

From the `dev` directory, run:

```bash
docker compose up -d
```

This will:
1. Start the MSSQL Server container (`mssql-dev`)
2. Wait for the database to be healthy
3. Automatically run the setup container (`mssql-dev-setup`) which executes `init.sql`

### Stopping the Database

```bash
docker compose down
```

To also remove the data volume:

```bash
docker compose down -v
```

### Viewing Logs

```bash
docker compose logs -f
```

## What `init.sql` Does

The initialization script (`init.sql`) performs the following setup steps:

1. **Database Creation**: Creates the application database (if it doesn't exist)
2. **CDC Setup**: Enables Change Data Capture at the database level
3. **User Creation**: Creates a SQL Server login and database user with appropriate permissions
4. **Create PowerSync Checkpoints table**: Creates the required `_powersync_checkpoints` table.
5. **Demo Tables**: Creates sample tables (`lists` and `todos`) for testing (optional examples)
6. **CDC Table Enablement**: Enables CDC tracking on the demo tables
7. **Permissions**: Grants `db_datareader` and `cdc_reader` roles to the application user
8. **Sample Data**: Inserts initial test data into the `lists` table

All operations are idempotent, so you can safely re-run the setup without errors. The demo tables section (steps 5–7) serves as an example of how to enable CDC on your own tables.

## Connection Details

- **Host**: `localhost`
- **Port**: `1433`
- **SA Login**: `sa` / `{ROOT_PASSWORD}`
- **App Login**: `{DB_USER}` / `{DB_USER_PASSWORD}`
- **Database**: `{DATABASE}`

## Troubleshooting

- If the setup container fails, check logs: `docker compose logs mssql-dev-setup`
- Ensure your `.env` file exists and contains all required variables
- The database container may take 30–60 seconds to become healthy on the first startup
- If you encounter connection issues, verify the container is running: `docker compose ps`
