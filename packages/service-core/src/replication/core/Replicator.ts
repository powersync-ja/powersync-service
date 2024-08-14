/**
 *   A replicator manages the mechanics for replicating data from a data source to powersync.
 *   This includes copying across the original data set and then keeping it in sync with the data source.
 *   It should also handle any changes to the sync rules -> TODO: For now
 */
export interface Replicator {
  /**
   *  Unique identifier for this replicator
   */
  id: string;
  start(): void;
  stop(): Promise<void>;
}
