export function unsupportedMikroOrmStorageFeature(feature: string): never {
  throw new Error(`MikroORM bucket storage does not implement ${feature} yet.`);
}
