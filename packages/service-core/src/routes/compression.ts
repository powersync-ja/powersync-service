import { PassThrough, pipeline, Readable, Transform } from 'node:stream';
import type Negotiator from 'negotiator';
import * as zlib from 'node:zlib';
import { RequestTracker } from '../sync/RequestTracker.js';

/**
 * Compress a streamed response.
 *
 * `@fastify/compress` can do something similar, but does not appear to work as well on streamed responses.
 * The manual implementation is simple enough, and gives us more control over the low-level details.
 *
 * @param negotiator Negotiator from the request, to negotiate response encoding
 * @param stream plain-text stream
 * @returns
 */
export function maybeCompressResponseStream(
  negotiator: Negotiator,
  stream: Readable,
  tracker: RequestTracker
): { stream: Readable; encodingHeaders: { 'content-encoding'?: string } } {
  const encoding = (negotiator as any).encoding(['identity', 'gzip', 'zstd'], { preferred: 'zstd' });
  if (encoding == 'zstd') {
    tracker.setCompressed(encoding);
    return {
      stream: transform(
        stream,
        // Available since Node v23.8.0, v22.15.0
        // This does the actual compression in a background thread pool.
        zlib.createZstdCompress({
          // We need to flush the frame after every new input chunk, to avoid delaying data
          // in the output stream.
          flush: zlib.constants.ZSTD_e_flush,
          params: {
            // Default compression level is 3. We reduce this slightly to limit CPU overhead
            [zlib.constants.ZSTD_c_compressionLevel]: 2
          }
        }),
        tracker
      ),
      encodingHeaders: { 'content-encoding': 'zstd' }
    };
  } else if (encoding == 'gzip') {
    tracker.setCompressed(encoding);
    return {
      stream: transform(
        stream,
        zlib.createGzip({
          // We need to flush the frame after every new input chunk, to avoid delaying data
          // in the output stream.
          flush: zlib.constants.Z_SYNC_FLUSH
        }),
        tracker
      ),
      encodingHeaders: { 'content-encoding': 'gzip' }
    };
  } else {
    return {
      stream: stream,
      encodingHeaders: {}
    };
  }
}

function transform(source: Readable, transform: Transform, tracker: RequestTracker) {
  // pipe does not forward error events automatically, resulting in unhandled error
  // events. This forwards it.
  const out = new PassThrough();
  const trackingTransform = new Transform({
    transform(chunk, encoding, callback) {
      tracker.addCompressedDataSent(chunk.length);
      callback(null, chunk);
    }
  });
  pipeline(source, transform, trackingTransform, out, (err) => {
    if (err) out.destroy(err);
  });
  return out;
}
