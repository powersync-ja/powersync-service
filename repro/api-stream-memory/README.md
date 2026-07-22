# PowerSync API streaming memory reproduction

This harness reproduces many clients concurrently downloading large rows from a
locally-built PowerSync API runner. Docker runs only the databases. Replication
and API processes run from this repository so they can be profiled or debugged.

The source contains 14 rows with a 6 MiB base64 text column (84 MiB total). A
global stream sends all rows to every authenticated client. Defaults are 70 HTTP
clients and one completed initial sync per client.

## Prerequisites

- Docker with Compose v2
- The Node version from the repository's `.nvmrc`
- Corepack/pnpm dependencies installed for this workspace
- Optional: Clinic.js for Clinic Doctor profiling

Install Clinic.js globally and verify it is available with:

```bash
npm install -g clinic
clinic doctor --help
```

## Run the reproduction

From this directory:

```bash
nvm use
./repro.sh backend-up
./repro.sh build
./repro.sh replication
```

Wait until initial replication is complete (the snapshot has completed and a
checkpoint is available), then stop the replication runner with Ctrl-C. Run the
profiled API in a new terminal:

```bash
MAX_OLD_SPACE_MB=576 ./repro.sh api-profile
```

Run clients in another terminal:

```bash
CLIENTS=70 MODE=http ONCE=1 ./repro.sh clients
```

`ONCE=1` exits each client after its first complete checkpoint. Use `ONCE=0` to
leave all streams connected. Try `MODE=websocket` to exercise the RSocket path.

Each `api-profile` run creates a timestamped folder under `profiles/` containing:

- `memory.csv`: one-second RSS and virtual-memory samples for the API process
- a V8 `.heapprofile` when the process exits cleanly
- up to three `.heapsnapshot` files when V8 approaches the configured heap limit
- a Node diagnostic report if the process terminates with a fatal error

While the API is running, the printed `kill -USR2 <pid>` command writes an
additional Node diagnostic report without stopping it. Heap snapshots and
profiles can be opened in Chrome DevTools' **Memory** panel.

## Clinic.js Doctor

The following is the complete workflow for running the reproduction with Clinic
Doctor. Run all commands from this directory.

### 1. Install Clinic and build the service

```bash
nvm use
npm install -g clinic
clinic doctor --help
./repro.sh build
```

The global Clinic installation belongs to the active Node version, so run
`nvm use` before installing or invoking it.

### 2. Start and seed the databases

```bash
./repro.sh backend-up
```

Postgres initializes the 14 large rows the first time its volume is created. To
discard existing source and bucket data before another run, use
`./repro.sh reset` and then run `./repro.sh backend-up` again.

### 3. Populate MongoDB bucket storage

Start the replication runner in terminal 1:

```bash
./repro.sh replication
```

Wait until the logs show that the initial snapshot has completed and a
checkpoint is available. Stop this runner with Ctrl-C. Replication does not need
to remain running while the clients download the already-populated bucket.

### 4. Start the API under Clinic

In terminal 2:

```bash
MAX_OLD_SPACE_MB=576 ./repro.sh api-clinic
```

Production limits the process to 720 MiB and configures old space to 80% of
available memory. The equivalent local old-space limit is therefore 576 MiB:

`720 MiB * 0.8 = 576 MiB`.

The harness defaults all API commands to `MAX_OLD_SPACE_MB=576`, so the explicit
environment variable may be omitted. Using `MAX_OLD_SPACE_PERCENTAGE=80`
directly would take 80% of the local host's available memory and would not match
production unless the local process was also constrained to 720 MiB. Do not set
the percentage and fixed-MiB variables together. Wait until the API is listening
on port 8080.

### 5. Connect the clients

In terminal 3:

```bash
CLIENTS=70 MODE=http ONCE=1 ./repro.sh clients
```

`ONCE=1` stops each client after its first complete checkpoint. Use `ONCE=0` to
keep all streams connected, or `MODE=websocket` to exercise the RSocket path.

### 6. Stop Clinic and generate the report

After the client run, return to terminal 2 and press Ctrl-C exactly once. This
signals Clinic, which stops the API and then processes the captured data. Do not
press Ctrl-C again or close the terminal while Clinic is processing. Wait for:

```text
Generated HTML file is file:///.../<pid>.clinic-doctor.html
```

Clinic writes the raw data and HTML report to the timestamped directory printed
when `api-clinic` starts:

```text
profiles/clinic-<timestamp>/
```

Find and open the report on macOS with:

```bash
find profiles -name '*.clinic-doctor.html' -print
open profiles/clinic-<timestamp>/<pid>.clinic-doctor.html
```

If the API exits from OOM, leave the Clinic terminal open and wait for Clinic to
process the child-process exit.

### Recover a missing HTML report

If Clinic was interrupted during processing, the raw `.clinic-doctor` directory
is still usable. Generate the report manually:

```bash
clinic doctor --visualize-only \
  profiles/clinic-<timestamp>/<pid>.clinic-doctor

open profiles/clinic-<timestamp>/<pid>.clinic-doctor.html
```

For a long or remote run, intentionally save only the raw collection data:

```bash
CLINIC_COLLECT_ONLY=1 ./repro.sh api-clinic
```

After stopping it, use the same `clinic doctor --visualize-only` command to
generate the HTML locally.

The built-in `api-profile` command is better for near-OOM heap snapshots, while
Clinic Doctor provides a broader view of heap usage, garbage collection, CPU,
and event-loop behavior.

To run without profiling, use `./repro.sh api`. To rerun from an empty source and
bucket store, use `./repro.sh reset` followed by `./repro.sh backend-up` and the
replication step. `backend-down` preserves database volumes.

## Useful variations

```bash
# Use the production-equivalent old-space limit (also the harness default)
MAX_OLD_SPACE_MB=576 ./repro.sh api-profile

# Use the percentage directly only when the process has a 720 MiB memory limit
MAX_OLD_SPACE_PERCENTAGE=80 ./repro.sh api-profile

# Increase fan-out and keep connections open
CLIENTS=100 ONCE=0 ./repro.sh clients

# Record samples every 250 ms
SAMPLE_SECONDS=0.25 ./repro.sh api-profile
```

The static HS256 key and database credentials are intentionally local and
insecure. Do not expose these services outside a development machine.
