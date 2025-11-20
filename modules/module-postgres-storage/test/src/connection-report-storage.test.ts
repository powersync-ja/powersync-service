import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { POSTGRES_REPORT_STORAGE_FACTORY } from './util.js';
import { event_types } from '@powersync/service-types';
import { register, ReportUserData } from '@powersync/service-core-tests';
import { PostgresReportStorage } from '../../src/storage/PostgresReportStorage.js';
import { DateTimeValue } from '@powersync/service-sync-rules';

const factory = await POSTGRES_REPORT_STORAGE_FACTORY();
const userData = register.REPORT_TEST_USERS;
const dates = register.REPORT_TEST_DATES;

function removeVolatileFields(sdks: event_types.ClientConnection[]): Partial<event_types.ClientConnection>[] {
  return sdks.map((sdk) => {
    const { id, disconnected_at, connected_at, jwt_exp, ...rest } = sdk;
    return {
      ...rest
    };
  });
}

async function loadData(userData: ReportUserData, factory: PostgresReportStorage) {
  await factory.db.sql`
      INSERT INTO
        connection_report_events (
          user_id,
          client_id,
          connected_at,
          sdk,
          user_agent,
          jwt_exp,
          id,
          disconnected_at
        )
      VALUES
        (
          ${{ type: 'varchar', value: userData.user_one.user_id }},
          ${{ type: 'varchar', value: userData.user_one.client_id }},
          ${{ type: 1184, value: userData.user_one.connected_at.toISOString() }},
          ${{ type: 'varchar', value: userData.user_one.sdk }},
          ${{ type: 'varchar', value: userData.user_one.user_agent }},
          ${{ type: 1184, value: userData.user_one.jwt_exp.toISOString() }},
          ${{ type: 'varchar', value: '1' }},
          NULL
        ),
        (
          ${{ type: 'varchar', value: userData.user_two.user_id }},
          ${{ type: 'varchar', value: userData.user_two.client_id }},
          ${{ type: 1184, value: userData.user_two.connected_at.toISOString() }},
          ${{ type: 'varchar', value: userData.user_two.sdk }},
          ${{ type: 'varchar', value: userData.user_two.user_agent }},
          ${{ type: 1184, value: userData.user_two.jwt_exp.toISOString() }},
          ${{ type: 'varchar', value: '2' }},
          NULL
        ),
        (
          ${{ type: 'varchar', value: userData.user_four.user_id }},
          ${{ type: 'varchar', value: userData.user_four.client_id }},
          ${{ type: 1184, value: userData.user_four.connected_at.toISOString() }},
          ${{ type: 'varchar', value: userData.user_four.sdk }},
          ${{ type: 'varchar', value: userData.user_four.user_agent }},
          ${{ type: 1184, value: userData.user_four.jwt_exp.toISOString() }},
          ${{ type: 'varchar', value: '4' }},
          NULL
        ),
        (
          ${{ type: 'varchar', value: userData.user_old.user_id }},
          ${{ type: 'varchar', value: userData.user_old.client_id }},
          ${{ type: 1184, value: userData.user_old.connected_at.toISOString() }},
          ${{ type: 'varchar', value: userData.user_old.sdk }},
          ${{ type: 'varchar', value: userData.user_old.user_agent }},
          ${{ type: 1184, value: userData.user_old.jwt_exp.toISOString() }},
          ${{ type: 'varchar', value: '5' }},
          NULL
        ),
        (
          ${{ type: 'varchar', value: userData.user_three.user_id }},
          ${{ type: 'varchar', value: userData.user_three.client_id }},
          ${{ type: 1184, value: userData.user_three.connected_at.toISOString() }},
          ${{ type: 'varchar', value: userData.user_three.sdk }},
          ${{ type: 'varchar', value: userData.user_three.user_agent }},
          NULL,
          ${{ type: 'varchar', value: '3' }},
          ${{ type: 1184, value: userData.user_three.disconnected_at.toISOString() }}
        ),
        (
          ${{ type: 'varchar', value: userData.user_week.user_id }},
          ${{ type: 'varchar', value: userData.user_week.client_id }},
          ${{ type: 1184, value: userData.user_week.connected_at.toISOString() }},
          ${{ type: 'varchar', value: userData.user_week.sdk }},
          ${{ type: 'varchar', value: userData.user_week.user_agent }},
          NULL,
          ${{ type: 'varchar', value: 'week' }},
          ${{ type: 1184, value: userData.user_week.disconnected_at.toISOString() }}
        ),
        (
          ${{ type: 'varchar', value: userData.user_month.user_id }},
          ${{ type: 'varchar', value: userData.user_month.client_id }},
          ${{ type: 1184, value: userData.user_month.connected_at.toISOString() }},
          ${{ type: 'varchar', value: userData.user_month.sdk }},
          ${{ type: 'varchar', value: userData.user_month.user_agent }},
          NULL,
          ${{ type: 'varchar', value: 'month' }},
          ${{ type: 1184, value: userData.user_month.disconnected_at.toISOString() }}
        ),
        (
          ${{ type: 'varchar', value: userData.user_expired.user_id }},
          ${{ type: 'varchar', value: userData.user_expired.client_id }},
          ${{ type: 1184, value: userData.user_expired.connected_at.toISOString() }},
          ${{ type: 'varchar', value: userData.user_expired.sdk }},
          ${{ type: 'varchar', value: userData.user_expired.user_agent }},
          ${{ type: 1184, value: userData.user_expired.jwt_exp.toISOString() }},
          ${{ type: 'varchar', value: 'expired' }},
          NULL
        )
    `.execute();
}

async function deleteData(factory: PostgresReportStorage) {
  await factory.db.sql`TRUNCATE TABLE connection_report_events`.execute();
}

beforeAll(async () => {
  await loadData(userData, factory);
});
afterAll(async () => {
  await deleteData(factory);
});

describe('Report storage tests', async () => {
  await register.registerReportTests(factory);
});

describe('Connection report storage', async () => {
  it('Should update a connection event if its within a day', async () => {
    const newConnectAt = new Date(
      dates.now.getFullYear(),
      dates.now.getMonth(),
      dates.now.getDate(),
      dates.now.getHours(),
      dates.now.getMinutes() + 20
    );
    const jwtExp = new Date(newConnectAt.getFullYear(), newConnectAt.getMonth(), newConnectAt.getDate() + 1);
    await factory.reportClientConnection({
      sdk: userData.user_one.sdk,
      connected_at: newConnectAt,
      jwt_exp: jwtExp,
      client_id: userData.user_one.client_id,
      user_id: userData.user_one.user_id,
      user_agent: userData.user_one.user_agent
    });

    const sdk = await factory.db
      .sql`SELECT * FROM connection_report_events WHERE user_id = ${{ type: 'varchar', value: userData.user_one.user_id }} AND client_id = ${{ type: 'varchar', value: userData.user_one.client_id }}`.rows<event_types.ClientConnection>();
    expect(sdk).toHaveLength(1);
    expect(new Date((sdk[0].connected_at as unknown as DateTimeValue).iso8601Representation).toISOString()).toEqual(
      newConnectAt.toISOString()
    );
    expect(new Date((sdk[0].jwt_exp! as unknown as DateTimeValue).iso8601Representation).toISOString()).toEqual(
      jwtExp.toISOString()
    );
    expect(sdk[0].disconnected_at).toBeNull();
    const cleaned = removeVolatileFields(sdk);
    expect(cleaned).toMatchSnapshot();
  });

  it('Should update a connection event and make it disconnected', async () => {
    const disconnectAt = new Date(
      dates.now.getFullYear(),
      dates.now.getMonth(),
      dates.now.getDate(),
      dates.now.getHours(),
      dates.now.getMinutes() + 20
    );
    const jwtExp = new Date(disconnectAt.getFullYear(), disconnectAt.getMonth(), disconnectAt.getDate() + 1);

    await factory.reportClientDisconnection({
      disconnected_at: disconnectAt,
      jwt_exp: jwtExp,
      client_id: userData.user_three.client_id,
      user_id: userData.user_three.user_id,
      user_agent: userData.user_three.user_agent,
      connected_at: dates.yesterday
    });

    const sdk = await factory.db
      .sql`SELECT * FROM connection_report_events WHERE user_id = ${{ type: 'varchar', value: userData.user_three.user_id }}`.rows<event_types.ClientConnection>();
    expect(sdk).toHaveLength(1);
    expect(new Date((sdk[0].disconnected_at! as unknown as DateTimeValue).iso8601Representation).toISOString()).toEqual(
      disconnectAt.toISOString()
    );
    const cleaned = removeVolatileFields(sdk);
    expect(cleaned).toMatchSnapshot();
  });

  it('Should create a connection event if its after a day', async () => {
    const newConnectAt = new Date(
      dates.now.getFullYear(),
      dates.now.getMonth(),
      dates.now.getDate() + 1,
      dates.now.getHours()
    );
    const jwtExp = new Date(newConnectAt.getFullYear(), newConnectAt.getMonth(), newConnectAt.getDate() + 1);

    await factory.reportClientConnection({
      sdk: userData.user_week.sdk,
      connected_at: newConnectAt,
      jwt_exp: jwtExp,
      client_id: userData.user_week.client_id,
      user_id: userData.user_week.user_id,
      user_agent: userData.user_week.user_agent
    });

    const sdk = await factory.db
      .sql`SELECT * FROM connection_report_events WHERE user_id = ${{ type: 'varchar', value: userData.user_week.user_id }}`.rows<event_types.ClientConnection>();
    expect(sdk).toHaveLength(2);
    const cleaned = removeVolatileFields(sdk);
    expect(cleaned).toMatchSnapshot();
  });

  it('Should delete rows older than specified range', async () => {
    await deleteData(factory);
    await loadData(userData, factory);
    await factory.deleteOldConnectionData({
      date: dates.weekAgo
    });
    const sdk = await factory.getClientConnectionReports({
      start: dates.monthAgo,
      end: dates.now
    });
    expect(sdk).toMatchSnapshot();
  });
});
