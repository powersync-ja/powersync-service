import { BaseObserver } from '@powersync/lib-services-framework';
import { DEFAULT_TAG } from '@powersync/service-sync-rules';
import { ConvexApiClient } from '../client/ConvexApiClient.js';
import { ResolvedConvexConnectionConfig } from '../types/types.js';

export interface ConvexConnectionManagerListener {
  onEnded?: () => void;
}

export class ConvexConnectionManager extends BaseObserver<ConvexConnectionManagerListener> {
  readonly client: ConvexApiClient;
  readonly schema = 'convex';
  readonly connectionTag: string;
  readonly connectionId: string;

  constructor(public readonly config: ResolvedConvexConnectionConfig) {
    super();
    this.client = new ConvexApiClient(config);
    this.connectionTag = config.tag ?? DEFAULT_TAG;
    this.connectionId = config.id ?? 'default';
  }

  async end(): Promise<void> {
    this.iterateListeners((listener) => {
      listener.onEnded?.();
    });
  }
}
