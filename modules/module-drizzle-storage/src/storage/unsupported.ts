export function unsupportedDrizzleStorageFeature(feature: string): never {
  throw new Error(`Drizzle bucket storage does not implement ${feature} yet.`);
}
