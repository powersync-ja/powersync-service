import { ServiceAssertionError } from '@powersync/lib-services-framework';
import { storage } from '@powersync/service-core';

export function postgresTableId(id: storage.SourceTableId) {
  if (typeof id == 'string') {
    return id;
  }
  throw new ServiceAssertionError(`Expected string table id, got ObjectId`);
}
