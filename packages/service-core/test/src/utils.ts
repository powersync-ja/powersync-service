import { SOURCE } from '@powersync/service-sync-rules';
/**
 * Removes the source property from an object.
 *
 * This is for tests where we don't care about this value, and it adds a lot of noise in the output.
 */
export function removeSource<T extends { source?: any }>(obj: T): Omit<T, 'source'> {
  const { source, ...rest } = obj;
  return rest;
}

/**
 * Removes the [SOURCE] symbol property from an object.
 *
 * This is for tests where we don't care about this value, and it adds a lot of noise in the output.
 */
export function removeSourceSymbol<T extends { [SOURCE]: any }>(obj: T): Omit<T, typeof SOURCE> {
  const { [SOURCE]: source, ...rest } = obj;
  return rest;
}
