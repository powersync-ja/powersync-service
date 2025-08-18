import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { POSTGRES_REPORT_STORAGE_FACTORY } from './util.js';
import { event_types } from '@powersync/service-types';

function removeVolatileFields(sdks: event_types.SdkConnection[]): Partial<event_types.SdkConnection>[] {
  return sdks.map((sdk) => {
    const { id, disconnected_at, connected_at, jwt_exp, ...rest } = sdk;
    return {
      ...rest
    };
  });
}

describe('SDK reporting storage', async () => {
  const factory = await POSTGRES_REPORT_STORAGE_FACTORY();
  const now = new Date();
  const nowAdd5minutes = new Date(
    now.getFullYear(),
    now.getMonth(),
    now.getDate(),
    now.getHours(),
    now.getMinutes() + 5
  );
  const nowLess5minutes = new Date(
    now.getFullYear(),
    now.getMonth(),
    now.getDate(),
    now.getHours(),
    now.getMinutes() - 5
  );
  const dayAgo = new Date(now.getFullYear(), now.getMonth(), now.getDate() - 1, now.getHours());
  const yesterday = new Date(now.getFullYear(), now.getMonth(), now.getDate() - 1);
  const weekAgo = new Date(now.getFullYear(), now.getMonth(), now.getDate() - 7);
  const monthAgo = new Date(now.getFullYear(), now.getMonth() - 1, now.getDate());
  const user_one = {
    user_id: 'user_one',
    client_id: 'client_one',
    connected_at: now.toISOString(),
    sdk: 'powersync-dart/1.6.4',
    user_agent: 'powersync-dart/1.6.4 Dart (flutter-web) Chrome/128 android',
    jwt_exp: nowAdd5minutes.toISOString(),
    id: '1'
  };
  const user_two = {
    user_id: 'user_two',
    client_id: 'client_two',
    connected_at: nowLess5minutes.toISOString(),
    sdk: 'powersync-js/1.21.1',
    user_agent: 'powersync-js/1.21.0 powersync-web Chromium/138 linux',
    jwt_exp: nowAdd5minutes.toISOString(),
    id: '2'
  };
  const user_three = {
    user_id: 'user_three',
    client_id: 'client_three',
    connected_at: yesterday.toISOString(),
    sdk: 'powersync-js/1.21.2',
    user_agent: 'powersync-js/1.21.0 powersync-web Firefox/141 linux',
    disconnected_at: yesterday.toISOString(),
    id: '3'
  };

  const user_four = {
    user_id: 'user_four',
    client_id: 'client_four',
    connected_at: now.toISOString(),
    sdk: 'powersync-js/1.21.4',
    user_agent: 'powersync-js/1.21.0 powersync-web Firefox/141 linux',
    jwt_exp: nowLess5minutes.toISOString(),
    id: '4'
  };

  const user_week = {
    user_id: 'user_week',
    client_id: 'client_week',
    connected_at: weekAgo.toISOString(),
    sdk: 'powersync-js/1.24.5',
    user_agent: 'powersync-js/1.21.0 powersync-web Firefox/141 linux',
    disconnected_at: weekAgo.toISOString(),
    id: 'week'
  };

  const user_month = {
    user_id: 'user_month',
    client_id: 'client_month',
    connected_at: monthAgo.toISOString(),
    sdk: 'powersync-js/1.23.6',
    user_agent: 'powersync-js/1.23.0 powersync-web Firefox/141 linux',
    disconnected_at: monthAgo.toISOString(),
    id: 'month'
  };

  const user_expired = {
    user_id: 'user_expired',
    client_id: 'client_expired',
    connected_at: monthAgo.toISOString(),
    sdk: 'powersync-js/1.23.7',
    user_agent: 'powersync-js/1.23.0 powersync-web Firefox/141 linux',
    jwt_exp: monthAgo.toISOString(),
    id: 'expired'
  };

  async function loadData() {
    await factory.db.sql`
      INSERT INTO
        sdk_report_events (
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
          ${{ type: 'varchar', value: user_one.user_id }},
          ${{ type: 'varchar', value: user_one.client_id }},
          ${{ type: 1184, value: user_one.connected_at }},
          ${{ type: 'varchar', value: user_one.sdk }},
          ${{ type: 'varchar', value: user_one.user_agent }},
          ${{ type: 1184, value: user_one.jwt_exp }},
          ${{ type: 'varchar', value: user_one.id }},
          NULL
        ),
        (
          ${{ type: 'varchar', value: user_two.user_id }},
          ${{ type: 'varchar', value: user_two.client_id }},
          ${{ type: 1184, value: user_two.connected_at }},
          ${{ type: 'varchar', value: user_two.sdk }},
          ${{ type: 'varchar', value: user_two.user_agent }},
          ${{ type: 1184, value: user_two.jwt_exp }},
          ${{ type: 'varchar', value: user_two.id }},
          NULL
        ),
        (
          ${{ type: 'varchar', value: user_four.user_id }},
          ${{ type: 'varchar', value: user_four.client_id }},
          ${{ type: 1184, value: user_four.connected_at }},
          ${{ type: 'varchar', value: user_four.sdk }},
          ${{ type: 'varchar', value: user_four.user_agent }},
          ${{ type: 1184, value: user_four.jwt_exp }},
          ${{ type: 'varchar', value: user_four.id }},
          NULL
        ),
        (
          ${{ type: 'varchar', value: user_three.user_id }},
          ${{ type: 'varchar', value: user_three.client_id }},
          ${{ type: 1184, value: user_three.connected_at }},
          ${{ type: 'varchar', value: user_three.sdk }},
          ${{ type: 'varchar', value: user_three.user_agent }},
          NULL,
          ${{ type: 'varchar', value: user_three.id }},
          ${{ type: 1184, value: user_three.disconnected_at }}
        ),
        (
          ${{ type: 'varchar', value: user_week.user_id }},
          ${{ type: 'varchar', value: user_week.client_id }},
          ${{ type: 1184, value: user_week.connected_at }},
          ${{ type: 'varchar', value: user_week.sdk }},
          ${{ type: 'varchar', value: user_week.user_agent }},
          NULL,
          ${{ type: 'varchar', value: user_week.id }},
          ${{ type: 1184, value: user_week.disconnected_at }}
        ),
        (
          ${{ type: 'varchar', value: user_month.user_id }},
          ${{ type: 'varchar', value: user_month.client_id }},
          ${{ type: 1184, value: user_month.connected_at }},
          ${{ type: 'varchar', value: user_month.sdk }},
          ${{ type: 'varchar', value: user_month.user_agent }},
          NULL,
          ${{ type: 'varchar', value: user_month.id }},
          ${{ type: 1184, value: user_month.disconnected_at }}
        ),
        (
          ${{ type: 'varchar', value: user_expired.user_id }},
          ${{ type: 'varchar', value: user_expired.client_id }},
          ${{ type: 1184, value: user_expired.connected_at }},
          ${{ type: 'varchar', value: user_expired.sdk }},
          ${{ type: 'varchar', value: user_expired.user_agent }},
          ${{ type: 1184, value: user_expired.jwt_exp }},
          ${{ type: 'varchar', value: user_expired.id }},
          NULL
        )
    `.execute();
  }

  function deleteData() {
    return factory.db.sql`TRUNCATE TABLE sdk_report_events`.execute();
  }

  beforeAll(async () => {
    await loadData();
  });

  afterAll(async () => {
    await deleteData();
  });
  it('Should show connected users with start range', async () => {
    const current = await factory.listCurrentConnections({
      range: {
        start: new Date(
          now.getFullYear(),
          now.getMonth(),
          now.getDate(),
          now.getHours(),
          now.getMinutes() - 1
        ).toISOString()
      }
    });
    expect(current).toMatchSnapshot();
  });
  it('Should show connected users with start range and end range', async () => {
    const current = await factory.listCurrentConnections({
      range: {
        end: nowLess5minutes.toISOString(),
        start: new Date(
          now.getFullYear(),
          now.getMonth(),
          now.getDate(),
          now.getHours(),
          now.getMinutes() - 6
        ).toISOString()
      }
    });
    expect(current).toMatchSnapshot();
  });
  it('Should show SDK scrape data for user over the past month', async () => {
    const sdk = await factory.scrapeSdkData({
      start: monthAgo,
      end: now
    });
    expect(sdk).toMatchSnapshot();
  });
  it('Should show SDK scrape data for user over the past week', async () => {
    const sdk = await factory.scrapeSdkData({
      start: weekAgo,
      end: now
    });
    expect(sdk).toMatchSnapshot();
  });
  it('Should show SDK scrape data for user over the past day', async () => {
    const sdk = await factory.scrapeSdkData({
      start: dayAgo,
      end: now
    });
    expect(sdk).toMatchSnapshot();
  });

  it('Should update a sdk event if its within a day', async () => {
    const newConnectAt = new Date(
      now.getFullYear(),
      now.getMonth(),
      now.getDate(),
      now.getHours(),
      now.getMinutes() + 20
    );
    const jwtExp = new Date(newConnectAt.getFullYear(), newConnectAt.getMonth(), newConnectAt.getDate() + 1);
    await factory.reportSdkConnect({
      sdk: user_one.sdk,
      connected_at: newConnectAt,
      jwt_exp: jwtExp,
      client_id: user_one.client_id,
      user_id: user_one.user_id,
      user_agent: user_one.user_agent
    });

    const sdk = await factory.db
      .sql`SELECT * FROM sdk_report_events WHERE user_id = ${{ type: 'varchar', value: user_one.user_id }}`.rows<event_types.SdkConnection>();
    expect(sdk).toHaveLength(1);
    expect(new Date(sdk[0].connected_at).toISOString()).toEqual(newConnectAt.toISOString());
    expect(new Date(sdk[0].jwt_exp!).toISOString()).toEqual(jwtExp.toISOString());
    expect(sdk[0].disconnected_at).toBeNull();
    const cleaned = removeVolatileFields(sdk);
    expect(cleaned).toMatchSnapshot();
  });

  it('Should update a connected sdk event and make it disconnected', async () => {
    const disconnectAt = new Date(
      now.getFullYear(),
      now.getMonth(),
      now.getDate(),
      now.getHours(),
      now.getMinutes() + 20
    );
    const jwtExp = new Date(disconnectAt.getFullYear(), disconnectAt.getMonth(), disconnectAt.getDate() + 1);

    await factory.reportSdkDisconnect({
      disconnected_at: disconnectAt,
      jwt_exp: jwtExp,
      client_id: user_three.client_id,
      user_id: user_three.user_id,
      user_agent: user_three.user_agent,
      connected_at: yesterday
    });

    const sdk = await factory.db
      .sql`SELECT * FROM sdk_report_events WHERE user_id = ${{ type: 'varchar', value: user_three.user_id }}`.rows<event_types.SdkConnection>();
    expect(sdk).toHaveLength(1);
    expect(new Date(sdk[0].disconnected_at!).toISOString()).toEqual(disconnectAt.toISOString());
    const cleaned = removeVolatileFields(sdk);
    expect(cleaned).toMatchSnapshot();
  });

  it('Should create a sdk event if its after a day', async () => {
    const newConnectAt = new Date(now.getFullYear(), now.getMonth(), now.getDate() + 1, now.getHours());
    const jwtExp = new Date(newConnectAt.getFullYear(), newConnectAt.getMonth(), newConnectAt.getDate() + 1);

    await factory.reportSdkConnect({
      sdk: user_week.sdk,
      connected_at: newConnectAt,
      jwt_exp: jwtExp,
      client_id: user_week.client_id,
      user_id: user_week.user_id,
      user_agent: user_week.user_agent
    });

    const sdk = await factory.db
      .sql`SELECT * FROM sdk_report_events WHERE user_id = ${{ type: 'varchar', value: user_week.user_id }}`.rows<event_types.SdkConnection>();
    expect(sdk).toHaveLength(2);
    const cleaned = removeVolatileFields(sdk);
    expect(cleaned).toMatchSnapshot();
  });

  it('Should delete rows older than specified range', async () => {
    await deleteData();
    await loadData();
    await factory.deleteOldSdkData({
      date: weekAgo
    });
    const sdk = await factory.scrapeSdkData({
      start: monthAgo,
      end: now
    });
    expect(sdk).toMatchSnapshot();
  });
});
