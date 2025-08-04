// import { afterAll, beforeAll, describe, expect, it } from 'vitest';
// import { INITIALIZED_MONGO_REPORT_STORAGE_FACTORY } from './util.js';
// import { event_types } from '@powersync/service-types';
//
// function removeVolatileFields(sdks: event_types.SdkConnectDocument[]): Partial<event_types.SdkConnectDocument>[] {
//   return sdks.map((sdk) => {
//     const { id, disconnect_at, connect_at, jwt_exp, ...rest } = sdk;
//     return {
//       ...rest
//     };
//   });
// }
//
// describe('SDK reporting storage', async () => {
//   const factory = await INITIALIZED_MONGO_REPORT_STORAGE_FACTORY();
//   const now = new Date();
//   const nowAdd5minutes = new Date(
//     now.getFullYear(),
//     now.getMonth(),
//     now.getDate(),
//     now.getHours(),
//     now.getMinutes() + 5
//   );
//   const nowLess5minutes = new Date(
//     now.getFullYear(),
//     now.getMonth(),
//     now.getDate(),
//     now.getHours(),
//     now.getMinutes() - 5
//   );
//   const yesterday = new Date(now.getFullYear(), now.getMonth(), now.getDate() - 1);
//   const weekAgo = new Date(now.getFullYear(), now.getMonth(), now.getDate() - 7);
//   const monthAgo = new Date(now.getFullYear(), now.getMonth() - 1, now.getDate());
//   const user_one = {
//     user_id: 'user_one',
//     client_id: 'client_one',
//     connect_at: now,
//     sdk: 'powersync-dart/1.6.4',
//     user_agent: 'powersync-dart/1.6.4 Dart (flutter-web) Chrome/128 android',
//     jwt_exp: nowAdd5minutes
//   };
//   const user_two = {
//     user_id: 'user_two',
//     client_id: 'client_two',
//     connect_at: nowLess5minutes,
//     sdk: 'powersync-js/1.21.1',
//     user_agent: 'powersync-js/1.21.0 powersync-web Chromium/138 linux',
//     jwt_exp: nowAdd5minutes
//   };
//   const user_three = {
//     user_id: 'user_three',
//     client_id: 'client_three',
//     connect_at: yesterday,
//     sdk: 'powersync-js/1.21.2',
//     user_agent: 'powersync-js/1.21.0 powersync-web Firefox/141 linux',
//     disconnect_at: yesterday
//   };
//
//   const user_four = {
//     user_id: 'user_four',
//     client_id: 'client_four',
//     connect_at: now,
//     sdk: 'powersync-js/1.21.4',
//     user_agent: 'powersync-js/1.21.0 powersync-web Firefox/141 linux',
//     jwt_exp: nowLess5minutes
//   };
//
//   const user_week = {
//     user_id: 'user_week',
//     client_id: 'client_week',
//     connect_at: weekAgo,
//     sdk: 'powersync-js/1.24.5',
//     user_agent: 'powersync-js/1.21.0 powersync-web Firefox/141 linux',
//     disconnect_at: weekAgo
//   };
//
//   const user_month = {
//     user_id: 'user_month',
//     client_id: 'client_month',
//     connect_at: monthAgo,
//     sdk: 'powersync-js/1.23.6',
//     user_agent: 'powersync-js/1.23.0 powersync-web Firefox/141 linux',
//     disconnect_at: monthAgo
//   };
//
//   const user_expired = {
//     user_id: 'user_expired',
//     client_id: 'client_expired',
//     connect_at: monthAgo,
//     sdk: 'powersync-js/1.23.7',
//     user_agent: 'powersync-js/1.23.0 powersync-web Firefox/141 linux',
//     jwt_exp: monthAgo
//   };
//
//   async function loadData() {
//     await factory.db.sdk_report_events.insertMany([
//       user_one,
//       user_two,
//       user_three,
//       user_four,
//       user_week,
//       user_month,
//       user_expired
//     ]);
//   }
//
//   function deleteData() {
//     return factory.db.sdk_report_events.deleteMany();
//   }
//
//   beforeAll(async () => {
//     await loadData();
//   });
//
//   afterAll(async () => {
//     await deleteData();
//   });
//   it('Should show connected users with start range', async () => {
//     const current = await factory.listCurrentConnections({
//       range: {
//         start_date: new Date(
//           now.getFullYear(),
//           now.getMonth(),
//           now.getDate(),
//           now.getHours(),
//           now.getMinutes() - 1
//         ).toISOString()
//       }
//     });
//     expect(current).toMatchSnapshot();
//   });
//   it('Should show connected users with start range and end range', async () => {
//     const current = await factory.listCurrentConnections({
//       range: {
//         end_date: nowLess5minutes.toISOString(),
//         start_date: new Date(
//           now.getFullYear(),
//           now.getMonth(),
//           now.getDate(),
//           now.getHours(),
//           now.getMinutes() - 6
//         ).toISOString()
//       }
//     });
//     expect(current).toMatchSnapshot();
//   });
//   it('Should show SDK scrape data for user over the past month', async () => {
//     const sdk = await factory.scrapeSdkData({
//       interval: 1,
//       timeframe: 'month'
//     });
//     expect(sdk).toMatchSnapshot();
//   });
//   it('Should show SDK scrape data for user over the past week', async () => {
//     const sdk = await factory.scrapeSdkData({
//       interval: 1,
//       timeframe: 'week'
//     });
//     expect(sdk).toMatchSnapshot();
//   });
//   it('Should show SDK scrape data for user over the past day', async () => {
//     const sdk = await factory.scrapeSdkData({
//       interval: 1,
//       timeframe: 'day'
//     });
//     expect(sdk).toMatchSnapshot();
//   });
//
//   it('Should update a sdk event if its within a day', async () => {
//     const newConnectAt = new Date(
//       now.getFullYear(),
//       now.getMonth(),
//       now.getDate(),
//       now.getHours(),
//       now.getMinutes() + 20
//     );
//     const jwtExp = new Date(newConnectAt.getFullYear(), newConnectAt.getMonth(), newConnectAt.getDate() + 1);
//     await factory.reportSdkConnect({
//       sdk: user_one.sdk,
//       connect_at: newConnectAt,
//       jwt_exp: jwtExp,
//       client_id: user_one.client_id,
//       user_id: user_one.user_id,
//       user_agent: user_one.user_agent
//     });
//
//     const sdk = await factory.db.sdk_report_events.find({ user_id: user_one.user_id }).toArray();
//     expect(sdk).toHaveLength(1);
//     expect(new Date(sdk[0].connect_at)).toEqual(newConnectAt);
//     expect(new Date(sdk[0].jwt_exp!)).toEqual(jwtExp);
//     expect(sdk[0].disconnect_at).toBeNull();
//     const cleaned = removeVolatileFields(sdk);
//     expect(cleaned).toMatchSnapshot();
//   });
//
//   it('Should update a connected sdk event and make it disconnected', async () => {
//     const disconnectAt = new Date(
//       now.getFullYear(),
//       now.getMonth(),
//       now.getDate(),
//       now.getHours(),
//       now.getMinutes() + 20
//     );
//     const jwtExp = new Date(disconnectAt.getFullYear(), disconnectAt.getMonth(), disconnectAt.getDate() + 1);
//
//     await factory.reportSdkDisconnect({
//       disconnect_at: disconnectAt,
//       jwt_exp: jwtExp,
//       client_id: user_one.client_id,
//       user_id: user_one.user_id,
//       user_agent: user_one.user_agent
//     });
//
//     const sdk = await factory.db.sdk_report_events.find({ user_id: user_one.user_id }).toArray();
//     expect(sdk).toHaveLength(1);
//     expect(new Date(sdk[0].disconnect_at!)).toEqual(disconnectAt);
//     const cleaned = removeVolatileFields(sdk);
//     expect(cleaned).toMatchSnapshot();
//   });
//
//   it('Should create a sdk event if its after a day', async () => {
//     const newConnectAt = new Date(now.getFullYear(), now.getMonth(), now.getDate() + 1, now.getHours());
//     const jwtExp = new Date(newConnectAt.getFullYear(), newConnectAt.getMonth(), newConnectAt.getDate() + 1);
//
//     await factory.reportSdkConnect({
//       sdk: user_week.sdk,
//       connect_at: newConnectAt,
//       jwt_exp: jwtExp,
//       client_id: user_week.client_id,
//       user_id: user_week.user_id,
//       user_agent: user_week.user_agent
//     });
//
//     const sdk = await factory.db.sdk_report_events.find({ user_id: user_one.user_id }).toArray();
//     expect(sdk).toHaveLength(2);
//     const cleaned = removeVolatileFields(sdk);
//     expect(cleaned).toMatchSnapshot();
//   });
//
//   it('Should delete rows older than specified range', async () => {
//     await deleteData();
//     await loadData();
//     await factory.deleteOldSdkData({
//       interval: 1,
//       timeframe: 'week'
//     });
//     const sdk = await factory.scrapeSdkData({
//       interval: 1,
//       timeframe: 'month'
//     });
//     expect(sdk).toMatchSnapshot();
//   });
// });
