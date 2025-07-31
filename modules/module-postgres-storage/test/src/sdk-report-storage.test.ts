import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { POSTGRES_REPORT_STORAGE_FACTORY } from './util.js';

describe('SDK reporting storage', () => {
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
  const yesterday = new Date(now.getFullYear(), now.getMonth(), now.getDate() - 1);
  const user_one = {
    user_id: 'user_one',
    client_id: 'client_one',
    connect_at: now.toISOString(),
    sdk: 'powersync-dart/1.6.4',
    user_agent: 'powersync-dart/1.6.4 Dart (flutter-web) Chrome/128 android',
    jwt_exp: nowAdd5minutes.toISOString(),
    id: '1'
  };
  const user_two = {
    user_id: 'user_two',
    client_id: 'client_two',
    connect_at: nowLess5minutes.toISOString(),
    sdk: 'powersync-js/1.21.0',
    user_agent: 'powersync-js/1.21.0 powersync-web Chromium/138 linux',
    jwt_exp: nowAdd5minutes.toISOString(),
    id: '2'
  };
  const user_three = {
    user_id: 'user_three',
    client_id: 'client_three',
    connect_at: yesterday.toISOString(),
    sdk: 'powersync-js/1.21.0',
    user_agent: 'powersync-js/1.21.0 powersync-web Firefox/141 linux',
    disconnect_at: yesterday.toISOString(),
    id: '3'
  };

  const user_four = {
    user_id: 'user_four',
    client_id: 'user_five',
    connect_at: now.toISOString(),
    sdk: 'powersync-js/1.21.0',
    user_agent: 'powersync-js/1.21.0 powersync-web Firefox/141 linux',
    jwt_exp: nowLess5minutes.toISOString(),
    id: '4'
  };

  beforeAll(async () => {
    const { db } = await POSTGRES_REPORT_STORAGE_FACTORY();
    const result = await db.sql`
      INSERT INTO
        sdk_report_events (
          user_id,
          client_id,
          connect_at,
          sdk,
          user_agent,
          jwt_exp,
          id,
          disconnect_at
        )
      VALUES
        (
          ${{ type: 'varchar', value: user_one.user_id }},
          ${{ type: 'varchar', value: user_one.client_id }},
          ${{ type: 1184, value: user_one.connect_at }},
          ${{ type: 'varchar', value: user_one.sdk }},
          ${{ type: 'varchar', value: user_one.user_agent }},
          ${{ type: 1184, value: user_one.jwt_exp }},
          ${{ type: 'varchar', value: user_one.id }},
          NULL
        ),
        (
          ${{ type: 'varchar', value: user_two.user_id }},
          ${{ type: 'varchar', value: user_two.client_id }},
          ${{ type: 1184, value: user_two.connect_at }},
          ${{ type: 'varchar', value: user_two.sdk }},
          ${{ type: 'varchar', value: user_two.user_agent }},
          ${{ type: 1184, value: user_two.jwt_exp }},
          ${{ type: 'varchar', value: user_two.id }},
          NULL
        ),
        (
          ${{ type: 'varchar', value: user_four.user_id }},
          ${{ type: 'varchar', value: user_four.client_id }},
          ${{ type: 1184, value: user_four.connect_at }},
          ${{ type: 'varchar', value: user_four.sdk }},
          ${{ type: 'varchar', value: user_four.user_agent }},
          ${{ type: 1184, value: user_four.jwt_exp }},
          ${{ type: 'varchar', value: user_four.id }},
          NULL
        ),
        (
          ${{ type: 'varchar', value: user_three.user_id }},
          ${{ type: 'varchar', value: user_three.client_id }},
          ${{ type: 1184, value: user_three.connect_at }},
          ${{ type: 'varchar', value: user_three.sdk }},
          ${{ type: 'varchar', value: user_three.user_agent }},
          NULL,
          ${{ type: 'varchar', value: user_three.id }},
          ${{ type: 1184, value: user_three.disconnect_at }}
        )
    `.execute();
    console.log(result);
  });
  afterAll(async () => {
    const { db } = await POSTGRES_REPORT_STORAGE_FACTORY();
    await db.sql`DELETE FROM sdk_report_events`.execute();
  });
  it('Should show currently connected users with start range', async () => {
    const factory = await POSTGRES_REPORT_STORAGE_FACTORY();
    const current = await factory.listCurrentConnections({
      range: {
        start_date: new Date(
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
  it('Should show currently connected users with start range and end range', async () => {
    const factory = await POSTGRES_REPORT_STORAGE_FACTORY();
    const current = await factory.listCurrentConnections({
      range: {
        end_date: nowLess5minutes.toISOString(),
        start_date: new Date(
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
});
