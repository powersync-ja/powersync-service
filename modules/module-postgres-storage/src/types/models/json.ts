import { JsonContainer } from '@powersync/service-jsonbig';
import { Codec, codec } from 'ts-codec';

/**
 * Wraps a codec to support {@link JsonContainer} values.
 *
 * Because our postgres client implementation wraps JSON objects in a {@link JsonContainer}, this intermediate layer is
 * required to use JSON columns from Postgres in `ts-codec` models.
 *
 * Note that this serializes and deserializes values using {@link JSON}, so bigints are not supported.
 */
export function jsonContainerObject<I, O>(inner: Codec<I, O>): Codec<I, JsonContainer> {
  return codec(
    inner._tag,
    (input) => {
      return new JsonContainer(JSON.stringify(inner.encode(input)));
    },
    (json) => {
      if (!(json instanceof JsonContainer)) {
        throw new Error('Expected JsonContainer');
      }

      return inner.decode(JSON.parse(json.data));
    }
  );
}
