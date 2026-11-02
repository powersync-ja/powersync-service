import { ReplicatedGTID } from '@module/common/ReplicatedGTID.js';
import { MySQLConnectionManager } from '@module/replication/MySQLConnectionManager.js';
import { BinLogListener } from '@module/replication/zongji/BinLogListener.js';
import { ZongJi } from '@powersync/mysql-zongji';
import { EventEmitter } from 'node:events';
import { setImmediate } from 'node:timers/promises';
import { expect, test, vi } from 'vitest';
import { TEST_CONNECTION_OPTIONS, TestBinLogEventHandler } from './util.js';

test.each(['before', 'after'] as const)('restarts when the queue drains %s Zongji stops', async (drainOrder) => {
  const manager = new MySQLConnectionManager({ ...TEST_CONNECTION_OPTIONS, binlog_queue_memory_limit: 1 }, {});
  const stopStarted = Promise.withResolvers<void>();
  const processEvent = Promise.withResolvers<void>();
  // Control the external listener's stop acknowledgement independently of the
  // real processing queue: MySQL can acknowledge KILL after the queue drains.
  const zongji = Object.assign(new EventEmitter(), {
    stopped: false,
    ctrlConnection: {},
    stop() {
      this.stopped = true;
      stopStarted.resolve();
    }
  });
  vi.spyOn(manager, 'createBinlogListener').mockImplementation(() => zongji as unknown as ZongJi);
  const handler = new TestBinLogEventHandler();
  vi.spyOn(handler, 'onKeepAlive').mockImplementation(() => processEvent.promise);
  const listener = new BinLogListener({
    connectionManager: manager,
    eventHandler: handler,
    sourceTables: [],
    serverId: 1,
    activeServerUuid: 'test',
    startGTID: ReplicatedGTID.ZERO('test')
  });
  const restart = vi.spyOn(listener, 'start').mockResolvedValue();

  try {
    const drained = listener.processingQueue.drain();
    zongji.emit('binlog', { getEventName: () => 'heartbeat', size: 1024 * 1024 });
    await stopStarted.promise;
    if (drainOrder === 'before') {
      processEvent.resolve();
      await drained;
      expect(listener.processingQueue.idle()).toBe(true);
      zongji.emit('stopped');
    } else {
      zongji.emit('stopped');
      // Let stopZongji return while the queue worker is still blocked.
      await setImmediate();
      expect(listener.processingQueue.running()).toBe(1);
      expect(restart).not.toHaveBeenCalled();
      processEvent.resolve();
    }
    await vi.waitFor(() => expect(restart).toHaveBeenCalledExactlyOnceWith(true));
    expect(listener.queueMemoryUsage).toBe(0);
  } finally {
    processEvent.resolve();
    zongji.emit('stopped');
    await listener.stop();
    await manager.end();
  }
});
