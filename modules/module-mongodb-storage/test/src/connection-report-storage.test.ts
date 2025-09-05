import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { INITIALIZED_MONGO_REPORT_STORAGE_FACTORY } from './util.js';
import { register, ReportUserData } from '@powersync/service-core-tests';
import { event_types } from '@powersync/service-types';
import { MongoReportStorage } from '@module/storage/MongoReportStorage.js';

const userData = register.REPORT_TEST_USERS;
const dates = register.REPORT_TEST_DATES;
const factory = await INITIALIZED_MONGO_REPORT_STORAGE_FACTORY();

function removeVolatileFields(
  connections: event_types.ClientConnection[]
): Partial<event_types.ClientConnection & { _id: string }>[] {
  return connections.map((sdk: Partial<event_types.ClientConnection & { _id: string }>) => {
    const { _id, disconnected_at, connected_at, jwt_exp, ...rest } = sdk;
    return {
      ...rest
    };
  });
}

async function loadData(data: ReportUserData, factory: MongoReportStorage) {
  await factory.db.connection_report_events.insertMany(Object.values(data));
}

async function deleteData(factory: MongoReportStorage) {
  await factory.db.connection_report_events.deleteMany();
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

describe('Connection reporting storage', async () => {
  it('Should create a connection report if its after a day', async () => {
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

    const connection = await factory.db.connection_report_events.find({ user_id: userData.user_week.user_id }).toArray();
    expect(connection).toHaveLength(2);
    const cleaned = removeVolatileFields(connection);
    expect(cleaned).toMatchSnapshot();
  });

  it('Should update a connection report if its within a day', async () => {
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

    const connection = await factory.db.connection_report_events
      .find({ user_id: userData.user_one.user_id, client_id: userData.user_one.client_id })
      .toArray();
    expect(connection).toHaveLength(1);
    expect(new Date(connection[0].connected_at)).toEqual(newConnectAt);
    expect(new Date(connection[0].jwt_exp!)).toEqual(jwtExp);
    expect(connection[0].disconnected_at).toBeUndefined();
    const cleaned = removeVolatileFields(connection);
    expect(cleaned).toMatchSnapshot();
  });

  it('Should update a connected connection report and make it disconnected', async () => {
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
      connected_at: userData.user_three.connected_at
    });

    const connection = await factory.db.connection_report_events.find({ user_id: userData.user_three.user_id }).toArray();
    expect(connection).toHaveLength(1);
    expect(new Date(connection[0].disconnected_at!)).toEqual(disconnectAt);
    const cleaned = removeVolatileFields(connection);
    expect(cleaned).toMatchSnapshot();
  });

  it('Should delete rows older than specified range', async () => {
    await deleteData(factory);
    await loadData(userData, factory);
    await factory.deleteOldConnectionData({
      date: dates.weekAgo
    });
    const connection = await factory.getClientConnectionReports({
      start: dates.monthAgo,
      end: dates.now
    });
    expect(connection).toMatchSnapshot();
  });
});
