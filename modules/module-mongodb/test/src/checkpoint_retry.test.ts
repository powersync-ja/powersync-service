import { describe, expect, test } from 'vitest';

import { mongo } from '@powersync/lib-service-mongodb';

import { MongoLSN } from '@module/common/MongoLSN.js';
import { createCheckpoint } from '@module/replication/MongoRelation.js';
import { clearTestDb, connectMongoData, requireFailCommand } from './util.js';

describe('checkpoint retryable writes', () => {
  test('returns the persisted checkpoint clusterTime after a retryable write', { repeats: 1 }, async (ctx) => {
    // This test relies on very specific timing:
    //
    // 1. _powersync_checkpoints findAndModify command starts and is sent to the server
    // 2. command is written to the server
    // 3. _powersync_checkpoints findAndModify command fails with a retryable error (e.g. connection timeout)
    // 4. command is retried
    // 5. another write lands on the server, increasing the server's operationTime
    // 6. command succeeds with a new operationTime, but without actually writing again (server de-duplicates based on transaction id)
    //
    // It is quite difficult to simulate this, even with failCommand. We currently rely on triggering a socket timeout for the first command,
    // with another write happening right after that.

    const TIMEOUT = 200;
    const INSERT_COUNT = 5;

    const { db, client } = await connectMongoData({
      monitorCommands: true,
      socketTimeoutMS: TIMEOUT,
      maxPoolSize: 1
    });

    await using _client = { [Symbol.asyncDispose]: async () => await client.close() };
    await client.connect();
    await using failCommand = await requireFailCommand(client, ctx);

    await clearTestDb(db);

    const checkpointId = new mongo.ObjectId();
    const advanceClusterTime = db.collection('_advance_cluster_time');

    const changeStream = db.watch([], { maxAwaitTimeMS: TIMEOUT * 0.8 });
    await using _changeStream = { [Symbol.asyncDispose]: async () => await changeStream.close() };

    const checkpointEventPromise = waitForCheckpointEvent(changeStream, checkpointId, INSERT_COUNT).catch((e) =>
      console.error(e)
    );

    const commandsStarted: mongo.CommandStartedEvent[] = [];

    client.on('commandStarted', (event) => {
      commandsStarted.push(event);
    });

    const DEBUG_COMMANDS = false;
    if (DEBUG_COMMANDS) {
      // For debugging: Log all commands
      client.on('commandStarted', (event) => {
        console.log('commandStarted:', event);
      });

      client.on('commandSucceeded', (event) => {
        console.log('commandSucceeded:', event);
      });

      client.on('commandFailed', (event) => {
        console.log('commandFailed:', event);
      });
    }

    await failCommand.configure({
      mode: { times: 1 },
      data: {
        failCommands: ['commitTransaction', 'findAndModify'],
        blockConnection: true,
        blockTimeMS: TIMEOUT
      }
    });
    const returnedLsnPromise = createCheckpoint(client, db, checkpointId);

    for (let i = 0; i < INSERT_COUNT; i++) {
      await advanceClusterTime.insertOne({ at: new Date() });
    }

    const returnedLsn = await returnedLsnPromise;
    const checkpointEvent = await checkpointEventPromise;
    if (checkpointEvent == null) {
      throw new Error('change stream failed');
    }

    const eventLsn = new MongoLSN({
      timestamp: checkpointEvent.clusterTime!,
      resume_token: checkpointEvent._id
    }).comparable;

    if (DEBUG_COMMANDS) {
      const commands = commandsStarted
        .filter((c) => ['findAndModify', 'insert'].includes(c.commandName))
        .map((c) => c.commandName);
      // In the failure case, we had findAndModify, insert, findAndModify, potentially with more inserts in between.
      // With updated behavior, we only have a single findAndModify.
      console.log('Command order:', commands);
    }

    expect(eventLsn >= returnedLsn).toBe(true);
    expect(checkpointEvent.clusterTime).toEqual(MongoLSN.fromSerialized(returnedLsn).timestamp);
  });
});

async function waitForCheckpointEvent(
  changeStream: mongo.ChangeStream,
  checkpointId: mongo.ObjectId,
  insertCount: number
): Promise<mongo.ChangeStreamDocument | null> {
  let lastEvent: mongo.ChangeStreamDocument | null = null;
  let seenInserts = 0;
  for await (const event of changeStream) {
    if (
      (event.operationType == 'insert' || event.operationType == 'update' || event.operationType == 'replace') &&
      checkpointId.equals(event.documentKey._id)
    ) {
      lastEvent = event;
    } else if (event.operationType == 'insert' && event.ns.coll === '_advance_cluster_time') {
      seenInserts++;
      if (seenInserts >= insertCount) {
        // We finish when we hit the expected number of inserts, since the number of checkpoint events are unknown.
        break;
      }
    }
  }
  return lastEvent;
}
