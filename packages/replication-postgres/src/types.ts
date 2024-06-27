import { ReplicationColumn, ReplicationIdentity } from '@powersync/service-core';

export interface ReplicaIdentityResult {
  columns: ReplicationColumn[];
  replicationIdentity: ReplicationIdentity;
}
