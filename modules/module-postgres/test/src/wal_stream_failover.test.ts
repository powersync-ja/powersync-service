import { MissingReplicationSlotError } from '@module/replication/MissingReplicationSlotError.js';
import * as types from '@module/types/types.js';
import { putOp } from '@powersync/service-core-tests';
import * as pgwire from '@powersync/service-jpgwire';
import { execFile } from 'node:child_process';
import { randomBytes } from 'node:crypto';
import { promisify } from 'node:util';
import { describe, expect, onTestFinished, test, vi } from 'vitest';
import { env } from './env.js';
import { INITIALIZED_STORAGE_FACTORY } from './util.js';
import { WalStreamTestContext } from './wal_stream_utils.js';

/**
 * Tests verifying that PowerSync can pick up a replication stream from a standby worker in a replicated PostgreSQL
 * setup (https://www.postgresql.org/docs/17/logical-replication-failover.html).
 *
 * These tests need a custom setup (and kill the primary), so they start their own containers with the docker CLI.
 * To enable them, set the `PG_FAILOVER_TEST_IMAGE` environment variable, e.g. `PG_FAILOVER_TEST_IMAGE=postgres:17`.
 *
 * Containers are removed after each test. If a test run is interrupted, this should clean them up:
 * `docker rm -f $(docker ps -aq --filter label=powersync-failover-test); docker network prune --filter label=powersync-failover-test`
 */
describe.skipIf(env.PG_FAILOVER_TEST_IMAGE == null)('replication slot failover', { timeout: 120_000 }, () => {
  const image = env.PG_FAILOVER_TEST_IMAGE!;
  const expectedMajorVersion = Number(/:(\d+)/.exec(image)![1]);

  test('does not enable failover by default before Postgres 17', async ({ skip }) => {
    skip(expectedMajorVersion >= 17, 'Postgres 17+ supports failover slots');

    const primary = await startPrimary(image);
    onTestFinished(async () => {
      await primary.remove();
    });

    await using context = await WalStreamTestContext.open(INITIALIZED_STORAGE_FACTORY.factory, {
      connectionOptions: primary.connectionConfig
    });
    const { pool } = context;
    await context.updateSyncRules(SYNC_CONFIG);
    await pool.query(`CREATE TABLE test_data(id text primary key, description text)`);
    await pool.query(`INSERT INTO test_data(id, description) VALUES('t1', 'before')`);

    await context.initializeReplication();
    expect(await context.getBucketData('synced|0[]')).toMatchObject([
      putOp('test_data', { id: 't1', description: 'before' })
    ]);

    expect(await context.walStream.checkFailoverSlotSupport()).toBe(false);
    const slot = await getSlot(pool, context.storage!.replicationStreamName);
    expect(slot).toBeDefined();
  });

  test('resumes replication on a promoted standby', async ({ skip }) => {
    skip(expectedMajorVersion < 17, 'Failover slots require Postgres 17+');

    await using cluster = await FailoverCluster.start(image);

    const { replicationStreamId } = await replicateOnPrimary(cluster, {}, async (context, slotName) => {
      expect((await getSlot(cluster.primaryPool, slotName)).failover).toBe(true);
      // Spamming pg_log_standby_snapshot makes the standy persist the replication slot faster, but isn't necessary
      // for the correctness of this test.
      await cluster.waitForSlotSync(
        slotName,
        async () => {
          await cluster.primaryPool.query(`SELECT pg_log_standby_snapshot()`);
          await context.getCheckpoint();
        },
        { timeout: 30_000, interval: 100 }
      );
    });

    // Written while replication is stopped. This must be picked up from the synced slot after failover.
    await cluster.primaryPool.query(`INSERT INTO test_data(id, description) VALUES('t3', 'gap')`);

    await cluster.waitForStandbyReplay();
    await cluster.failover();

    await using context = await WalStreamTestContext.open(INITIALIZED_STORAGE_FACTORY.factory, {
      doNotClear: true,
      connectionOptions: cluster.standbyConfig
    });
    await context.loadActiveSyncRules();

    // This should resume replicating without complaining about a missing slot.
    expect(context.storage!.replicationStreamId).toEqual(replicationStreamId);
    await context.replicateSnapshot();

    await context.pool.query(`INSERT INTO test_data(id, description) VALUES('t4', 'after')`);

    expect(await context.getBucketData('synced|0[]')).toMatchObject([
      putOp('test_data', { id: 't1', description: 'before' }),
      putOp('test_data', { id: 't2', description: 'streamed' }),
      putOp('test_data', { id: 't3', description: 'gap' }),
      putOp('test_data', { id: 't4', description: 'after' })
    ]);
  });

  test('loses the replication slot on failover when slot_failover is disabled', async ({ skip }) => {
    skip(expectedMajorVersion < 17, 'Failover slots require Postgres 17+');

    await using cluster = await FailoverCluster.start(image);

    const { slotName } = await replicateOnPrimary(cluster, { slotFailover: false });
    expect((await getSlot(cluster.primaryPool, slotName)).failover).toBe(false);

    await cluster.waitForStandbyReplay();
    expect(await getSlot(cluster.standbyPool, slotName)).toBeUndefined();
    await cluster.failover();

    await using context = await WalStreamTestContext.open(INITIALIZED_STORAGE_FACTORY.factory, {
      doNotClear: true,
      connectionOptions: cluster.standbyConfig
    });
    await context.loadActiveSyncRules();

    // In the service, this is handled by starting a new replication stream with a new snapshot.
    await expect(context.replicateSnapshot()).rejects.toThrow(MissingReplicationSlotError);
  });
});

/**
 * Snapshots and streams some data from the primary, then stops replication.
 */
async function replicateOnPrimary(
  cluster: FailoverCluster,
  options: { slotFailover?: boolean },
  beforeStop?: (context: WalStreamTestContext, slotName: string) => Promise<void>
) {
  await using context = await WalStreamTestContext.open(INITIALIZED_STORAGE_FACTORY.factory, {
    connectionOptions: cluster.primaryConfig,
    walStreamOptions: options
  });
  const { pool } = context;
  await context.updateSyncRules(SYNC_CONFIG);
  await pool.query(`CREATE TABLE test_data(id text primary key, description text)`);
  await pool.query(`INSERT INTO test_data(id, description) VALUES('t1', 'before')`);

  await context.initializeReplication();
  await pool.query(`INSERT INTO test_data(id, description) VALUES('t2', 'streamed')`);

  expect(await context.getBucketData('synced|0[]')).toMatchObject([
    putOp('test_data', { id: 't1', description: 'before' }),
    putOp('test_data', { id: 't2', description: 'streamed' })
  ]);
  const slotName = context.storage!.replicationStreamName;
  await beforeStop?.(context, slotName);

  return {
    slotName: slotName,
    replicationStreamId: context.storage!.replicationStreamId
  };
}

class FailoverCluster implements AsyncDisposable {
  readonly primaryConfig: types.NormalizedPostgresConnectionConfig;
  readonly standbyConfig: types.NormalizedPostgresConnectionConfig;
  readonly primaryPool: pgwire.PgClient;
  readonly standbyPool: pgwire.PgClient;
  private primaryStopped = false;

  /**
   * Starts a primary with a physical streaming replication standby, configured for logical slot synchronization.
   */
  static async start(image: string) {
    const network = `powersync-failover-${randomBytes(4).toString('hex')}`;
    await docker('network', 'create', '--label', DOCKER_LABEL, network);
    let primary: PostgresContainer | undefined;
    let standby: PostgresContainer | undefined;
    try {
      primary = await startPrimary(image, network);
      standby = await PostgresContainer.run([
        '--network',
        network,
        '--user',
        'postgres',
        '-e',
        `PGPASSWORD=${PG_PASSWORD}`,
        '--entrypoint',
        'bash',
        image,
        '-c',
        STANDBY_SCRIPT
      ]);
    } catch (e) {
      await primary?.remove();
      await docker('network', 'rm', network);
      throw e;
    }

    const cluster = new FailoverCluster(network, primary, standby);
    try {
      await cluster.configureSlotSync();
    } catch (e) {
      await cluster[Symbol.asyncDispose]();
      throw e;
    }
    return cluster;
  }

  private async configureSlotSync() {
    // Logical walsenders for failover slots wait for the standby to confirm WAL before sending changes, so that
    // the replicator can never be ahead of the standby.
    await this.primaryPool.query(`ALTER SYSTEM SET synchronized_standby_slots = '${STANDBY_SLOT}'`);
    await this.primaryPool.query(`SELECT pg_reload_conf()`);
    await vi.waitFor(async () => {
      expect(await getSlot(this.primaryPool, STANDBY_SLOT)).toMatchObject({ active: true });
    }, WAIT_OPTIONS);
  }

  private constructor(
    private network: string,
    private primary: PostgresContainer,
    private standby: PostgresContainer
  ) {
    this.primaryConfig = primary.connectionConfig;
    this.standbyConfig = standby.connectionConfig;
    this.primaryPool = pgwire.connectPgWirePool(this.primaryConfig);
    this.standbyPool = pgwire.connectPgWirePool(this.standbyConfig);
  }

  async waitForStandbyReplay() {
    const [{ lsn }] = pgwire.pgwireRows(await this.primaryPool.query(`SELECT pg_current_wal_lsn()::text AS lsn`));
    await vi.waitFor(async () => {
      const [row] = pgwire.pgwireRows(
        await this.standbyPool.query({
          statement: `SELECT pg_last_wal_replay_lsn()::text AS replay_lsn, pg_last_wal_replay_lsn() >= $1::pg_lsn AS replayed`,
          params: [{ type: 'varchar', value: lsn }]
        })
      );
      expect(row, `standby replay of ${lsn}`).toMatchObject({ replayed: true });
    }, WAIT_OPTIONS);
  }

  /**
   * Waits for the slot sync worker on the standby to persist the slot at the primary's position.
   *
   * Synced slots stay temporary until the slot on the primary has advanced past the standby's catalog_xmin, and
   * temporary slots are dropped on promotion.
   *
   * @param advance called on every attempt, to advance the slot on the primary while replication is running.
   */
  async waitForSlotSync(slotName: string, advance: () => Promise<void>, options = WAIT_OPTIONS) {
    await vi.waitFor(async () => {
      await advance();
      const primarySlot = await getSlot(this.primaryPool, slotName);
      const [row] = pgwire.pgwireRows(
        await this.standbyPool.query({
          statement: `SELECT synced, temporary, invalidation_reason, confirmed_flush_lsn::text, catalog_xmin::text
                      FROM pg_replication_slots WHERE slot_name = $1`,
          params: [
            { type: 'varchar', value: slotName },
            { type: 'varchar', value: String(primarySlot.confirmed_flush_lsn) }
          ]
        })
      );
      expect(row, `synced slot ${slotName} on the standby`).toMatchObject({
        synced: true,
        temporary: false,
        invalidation_reason: null
      });
    }, options);
  }

  /**
   * Kills the primary and promotes the standby.
   */
  async failover() {
    await this.primaryPool.end();
    // Force-removing sends SIGKILL, so the primary doesn't get to shut down cleanly.
    await this.primary.remove();
    this.primaryStopped = true;
    await this.standbyPool.query(`SELECT pg_promote(true, 60)`);
  }

  async [Symbol.asyncDispose]() {
    if (!this.primaryStopped) {
      await this.primaryPool.end();
      await this.primary.remove();
    }
    await this.standbyPool.end();
    await this.standby.remove();
    await docker('network', 'rm', this.network);
  }
}

async function startPrimary(image: string, network?: string) {
  return PostgresContainer.run([
    ...(network ? ['--network', network, '--network-alias', PRIMARY_ALIAS] : []),
    '-e',
    `POSTGRES_PASSWORD=${PG_PASSWORD}`,
    '-e',
    `POSTGRES_DB=${PG_DATABASE}`,
    '--entrypoint',
    'bash',
    image,
    '-c',
    PRIMARY_SCRIPT
  ]);
}

/**
 * A Postgres container with its port published on localhost.
 */
class PostgresContainer {
  private constructor(
    readonly id: string,
    readonly connectionConfig: types.NormalizedPostgresConnectionConfig
  ) {}

  /**
   * Runs a container and waits until it accepts connections.
   *
   * @param args `docker run` arguments, including the image and command.
   */
  static async run(args: string[]) {
    const id = (
      await docker('run', '--detach', '--rm', '--label', DOCKER_LABEL, '--publish', '127.0.0.1::5432', ...args)
    ).trim();
    try {
      // Prints e.g. 127.0.0.1:55012
      const address = (await docker('port', id, '5432/tcp')).trim().split('\n')[0];
      const port = address.slice(address.lastIndexOf(':') + 1);
      const connectionConfig = types.normalizeConnectionConfig({
        type: 'postgresql',
        uri: `postgres://postgres:${PG_PASSWORD}@127.0.0.1:${port}/${PG_DATABASE}`,
        sslmode: 'disable'
      });
      const container = new PostgresContainer(id, connectionConfig);
      await container.waitForConnections();
      return container;
    } catch (e) {
      await docker('rm', '--force', id);
      throw e;
    }
  }

  /**
   * The image entrypoint initializes the database with a temporary server that doesn't listen on TCP, so a successful
   * connection means that the actual server is up.
   */
  private async waitForConnections() {
    try {
      await vi.waitFor(async () => {
        const db = await pgwire.connectPgWire(this.connectionConfig, { applicationName: 'powersync-tests' });
        await db.end();
      }, WAIT_OPTIONS);
    } catch (e) {
      const logs = await docker('logs', '--tail', '30', this.id).catch(() => '');
      throw new Error(`Container ${this.id.slice(0, 12)} did not accept connections. Container logs:\n${logs}`, {
        cause: e
      });
    }
  }

  async remove() {
    await docker('rm', '--force', this.id);
  }
}

async function docker(...args: string[]) {
  try {
    const { stdout } = await execFileAsync('docker', args);
    return stdout;
  } catch (e) {
    throw new Error(`docker ${args[0]} failed: ${(e as { stderr?: string }).stderr || e}`);
  }
}

async function getSlot(db: pgwire.PgClient, slotName: string) {
  const [slot] = pgwire.pgwireRows(
    await db.query({
      statement: `SELECT * FROM pg_replication_slots WHERE slot_name = $1`,
      params: [{ type: 'varchar', value: slotName }]
    })
  );
  return slot;
}

const SYNC_CONFIG = /* yaml */ `
  # sync config fixture.
  config:
    edition: 3
  streams:
    synced:
      query: SELECT id, description FROM test_data
`;

const execFileAsync = promisify(execFile);

const DOCKER_LABEL = 'powersync-failover-test';
const WAIT_OPTIONS = { timeout: 60_000, interval: 500 };
const PG_PASSWORD = 'postgres';
const PG_DATABASE = 'powersync_test';
const PRIMARY_ALIAS = 'primary';
const STANDBY_SLOT = 'standby_slot';

// Adds an init script allowing the standby to connect for physical replication, then runs the image's entrypoint.
const PRIMARY_SCRIPT = `
set -e
echo 'echo "host replication all all scram-sha-256" >> "$PGDATA/pg_hba.conf"' > /docker-entrypoint-initdb.d/replication-hba.sh
exec docker-entrypoint.sh postgres -c wal_level=logical
`;

// Clones the primary with a physical slot, then starts it as a hot standby with slot synchronization enabled.
// pg_basebackup -R writes primary_slot_name. primary_conninfo is overridden to include the dbname, which slot
// synchronization requires.
const STANDBY_SCRIPT = `
set -e
pg_basebackup -h ${PRIMARY_ALIAS} -U postgres -D "$PGDATA" -X stream -C -S ${STANDBY_SLOT} -R
chmod 0700 "$PGDATA"
exec postgres -D "$PGDATA" \\
  -c wal_level=logical \\
  -c hot_standby_feedback=on \\
  -c sync_replication_slots=on \\
  -c "primary_conninfo=host=${PRIMARY_ALIAS} user=postgres password=${PG_PASSWORD} dbname=${PG_DATABASE}"
`;
