export interface ReplicationChildClassDescriptor {
  readonly moduleUrl: string;
  readonly exportName: string;
  readonly constructorArgs: readonly unknown[];
}

export async function constructReplicationChildImplementation<Implementation>(
  descriptor: ReplicationChildClassDescriptor
): Promise<Implementation> {
  const module: Record<string, unknown> = await import(descriptor.moduleUrl);
  if (!Object.prototype.hasOwnProperty.call(module, descriptor.exportName)) {
    throw new Error(`Replication child module "${descriptor.moduleUrl}" does not export "${descriptor.exportName}"`);
  }

  const exported = module[descriptor.exportName];
  if (!isConstructible(exported)) {
    throw new Error(`Replication child module export "${descriptor.exportName}" is not constructible`);
  }
  return Reflect.construct(exported, descriptor.constructorArgs) as Implementation;
}

function isConstructible(value: unknown): value is new (...args: readonly unknown[]) => unknown {
  if (typeof value !== 'function') return false;
  try {
    Reflect.construct(Object, [], value);
    return true;
  } catch {
    return false;
  }
}
