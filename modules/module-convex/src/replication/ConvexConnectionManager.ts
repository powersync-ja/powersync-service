import { DEFAULT_TAG } from '@powersync/service-sync-rules';
import { ConvexApiClient } from '../client/ConvexApiClient.js';
import { ResolvedConvexConnectionConfig } from '../types/types.js';

export interface ConvexConnectionManagerListener {
  onEnded?: () => void;
}

export class ConvexConnectionManager {
  readonly client: ConvexApiClient;
  readonly schema = 'convex';
  readonly connectionTag: string;
  readonly connectionId: string;

  private listeners = new Set<ConvexConnectionManagerListener>();

  constructor(public readonly config: ResolvedConvexConnectionConfig) {
    this.client = new ConvexApiClient(config);
    this.connectionTag = config.tag ?? DEFAULT_TAG;
    this.connectionId = config.id ?? 'default';
  }

  registerListener(listener: ConvexConnectionManagerListener) {
    this.listeners.add(listener);
  }

  async end(): Promise<void> {
    for (const listener of [...this.listeners]) {
      listener.onEnded?.();
    }
    this.listeners.clear();
  }
}
