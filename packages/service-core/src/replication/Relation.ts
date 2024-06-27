export interface Relation {
  readonly relationId: number;
  readonly schema: string;
  readonly name: string;
  readonly replicaIdentity: ReplicationIdentity;
  readonly replicationColumns: ReplicationColumn[];
}

export type ReplicationIdentity = 'default' | 'nothing' | 'full' | 'index';

export interface ReplicationColumn {
  readonly name: string;
  readonly typeOid: number;
}
