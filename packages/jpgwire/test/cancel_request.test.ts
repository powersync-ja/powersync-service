import { afterEach, beforeEach, describe, expect, type MockInstance, test, vi } from 'vitest';
import { _net, pgconnection } from '../src/pgwire/mod.js';

describe('CancelRequest', () => {
  let connection: ReturnType<typeof pgconnection>;
  let socket: object;
  let write: MockInstance<typeof _net.write>;
  let close: MockInstance<typeof _net.closeNullable>;

  beforeEach(() => {
    connection = pgconnection();
    connection._backendKeyData = { pid: 123, secretKey: 456 };
    socket = {};
    vi.spyOn(_net, 'connect').mockResolvedValue(socket);
    write = vi.spyOn(_net, 'write').mockResolvedValue(undefined);
    close = vi.spyOn(_net, 'closeNullable').mockImplementation(() => {});
  });

  afterEach(() => vi.restoreAllMocks());

  test('waits for server EOF before completing and closing the cancellation socket', async () => {
    const eof = Promise.withResolvers<null>();
    const read = vi.spyOn(_net, 'read').mockResolvedValueOnce(1).mockReturnValueOnce(eof.promise);
    const completed = vi.fn();
    const cancellation = connection._cancelRequest().then(completed);

    try {
      // Even after draining data, cancellation must wait for the server to close
      // the socket so a late cancellation cannot affect the next query.
      await vi.waitFor(() => expect(read).toHaveBeenCalledTimes(2));
      expect(write).toHaveBeenCalledWith(
        socket,
        new Uint8Array([0, 0, 0, 16, 4, 210, 22, 46, 0, 0, 0, 123, 0, 0, 1, 200])
      );
      expect(completed).not.toHaveBeenCalled();
      expect(close).not.toHaveBeenCalled();
    } finally {
      eof.resolve(null);
      await cancellation;
    }

    expect(completed).toHaveBeenCalledOnce();
    expect(close).toHaveBeenCalledExactlyOnceWith(socket);
  });

  test('closes the cancellation socket if waiting for EOF fails', async () => {
    const error = new Error('cancel socket read failed');
    vi.spyOn(_net, 'read').mockRejectedValue(error);

    await expect(connection._cancelRequest()).rejects.toBe(error);
    expect(close).toHaveBeenCalledExactlyOnceWith(socket);
  });
});
