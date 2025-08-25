import { storage } from '@powersync/service-core';
import { expect, it } from 'vitest';

const now = new Date();
const nowAdd5minutes = new Date(now.getFullYear(), now.getMonth(), now.getDate(), now.getHours(), now.getMinutes() + 5);
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

export const REPORT_TEST_DATES = {
  now,
  nowAdd5minutes,
  nowLess5minutes,
  dayAgo,
  yesterday,
  weekAgo,
  monthAgo
};

const user_one = {
  user_id: 'user_one',
  client_id: 'client_one',
  connected_at: now,
  sdk: 'powersync-dart/1.6.4',
  user_agent: 'powersync-dart/1.6.4 Dart (flutter-web) Chrome/128 android',
  jwt_exp: nowAdd5minutes
};
const user_two = {
  user_id: 'user_two',
  client_id: 'client_two',
  connected_at: nowLess5minutes,
  sdk: 'powersync-js/1.21.1',
  user_agent: 'powersync-js/1.21.0 powersync-web Chromium/138 linux',
  jwt_exp: nowAdd5minutes
};
const user_three = {
  user_id: 'user_three',
  client_id: 'client_three',
  connected_at: yesterday,
  sdk: 'powersync-js/1.21.2',
  user_agent: 'powersync-js/1.21.0 powersync-web Firefox/141 linux',
  disconnected_at: yesterday
};

const user_four = {
  user_id: 'user_four',
  client_id: 'client_four',
  connected_at: now,
  sdk: 'powersync-js/1.21.4',
  user_agent: 'powersync-js/1.21.0 powersync-web Firefox/141 linux',
  jwt_exp: nowLess5minutes
};

const user_old = {
  user_id: 'user_one',
  client_id: '',
  connected_at: now,
  sdk: 'unknown',
  user_agent: 'Dart (flutter-web) Chrome/128 android',
  jwt_exp: nowAdd5minutes
};

const user_week = {
  user_id: 'user_week',
  client_id: 'client_week',
  connected_at: weekAgo,
  sdk: 'powersync-js/1.24.5',
  user_agent: 'powersync-js/1.21.0 powersync-web Firefox/141 linux',
  disconnected_at: weekAgo
};

const user_month = {
  user_id: 'user_month',
  client_id: 'client_month',
  connected_at: monthAgo,
  sdk: 'powersync-js/1.23.6',
  user_agent: 'powersync-js/1.23.0 powersync-web Firefox/141 linux',
  disconnected_at: monthAgo
};

const user_expired = {
  user_id: 'user_expired',
  client_id: 'client_expired',
  connected_at: monthAgo,
  sdk: 'powersync-js/1.23.7',
  user_agent: 'powersync-js/1.23.0 powersync-web Firefox/141 linux',
  jwt_exp: monthAgo
};
export const REPORT_TEST_USERS = {
  user_one,
  user_two,
  user_three,
  user_four,
  user_old,
  user_week,
  user_month,
  user_expired
};
export type ReportUserData = typeof REPORT_TEST_USERS;

export async function registerReportTests(factory: storage.ReportStorage) {
  it('Should show currently connected users', async () => {
    const current = await factory.getConnectedClients();
    expect(current).toMatchSnapshot();
  });

  it('Should show connection report data for user over the past month', async () => {
    const sdk = await factory.getClientConnectionReports({
      start: monthAgo,
      end: now
    });
    expect(sdk).toMatchSnapshot();
  });
  it('Should show connection report data for user over the past week', async () => {
    const sdk = await factory.getClientConnectionReports({
      start: weekAgo,
      end: now
    });
    expect(sdk).toMatchSnapshot();
  });
  it('Should show connection report data for user over the past day', async () => {
    const sdk = await factory.getClientConnectionReports({
      start: dayAgo,
      end: now
    });
    expect(sdk).toMatchSnapshot();
  });
}
