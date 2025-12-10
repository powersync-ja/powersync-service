import { BucketDataSource, BucketDataSourceDefinition, BucketParameterLookupSourceDefinition } from './BucketSource.js';
import { BucketIdTransformer, CompatibilityContext, CompatibilityOption, CreateSourceParams } from './index.js';

export interface BucketSourceState {
  /** The prefix is the bucket name before the parameters. */
  bucketPrefix: string;
}

export interface BucketParameterLookupSourceState {
  /** The lookup name + queryid is used to reference the parameter lookup record. */
  lookupName: string;
  queryId: string;
}

/**
 * Hydration state information for a source.
 *
 * This is what keeps track of bucket name and parameter lookup mappings for hydration. This can be used
 * both to re-use mappings across hydrations of different sync rule versions, or to generate new mappings.
 */
export interface HydrationState<
  T extends BucketSourceState = BucketSourceState,
  U extends BucketParameterLookupSourceState = BucketParameterLookupSourceState
> {
  /**
   * Given a bucket data source definition, get the bucket prefix to use for it.
   */
  getBucketSourceState(source: BucketDataSourceDefinition): T;

  /**
   * Given a bucket parameter lookup definition, get the persistence name to use.
   */
  getParameterLookupState(source: BucketParameterLookupSourceDefinition): U;
}

/**
 * This represents hydration state that performs no transformations.
 *
 * This is the legacy default behavior with no bucket versioning.
 */
export const DEFAULT_HYDRATION_STATE: HydrationState = {
  getBucketSourceState(source: BucketDataSourceDefinition) {
    return {
      bucketPrefix: source.defaultBucketPrefix
    };
  },
  getParameterLookupState(source) {
    return {
      lookupName: source.defaultLookupName,
      queryId: source.defaultQueryId
    };
  }
};

export function versionedHydrationState(version: number) {
  return new VersionedHydrationState((bucketId: string) => {
    return `${version}#${bucketId}`;
  });
}

export class VersionedHydrationState implements HydrationState {
  constructor(private transformer: BucketIdTransformer) {}

  getBucketSourceState(source: BucketDataSourceDefinition): BucketSourceState {
    return {
      bucketPrefix: this.transformer(source.defaultBucketPrefix)
    };
  }

  getParameterLookupState(source: BucketParameterLookupSourceDefinition): BucketParameterLookupSourceState {
    // No transformations applied here
    return {
      lookupName: source.defaultLookupName,
      queryId: source.defaultQueryId
    };
  }
}

export class BucketIdTransformerHydrationState implements HydrationState {
  constructor(private transformer: BucketIdTransformer) {}

  getBucketSourceState(source: BucketDataSourceDefinition): BucketSourceState {
    return {
      bucketPrefix: this.transformer(source.defaultBucketPrefix)
    };
  }

  getParameterLookupState(source: BucketParameterLookupSourceDefinition): BucketParameterLookupSourceState {
    // No transformations applied here
    return {
      lookupName: source.defaultLookupName,
      queryId: source.defaultQueryId
    };
  }
}

export function resolveHydrationState(params: CreateSourceParams): HydrationState {
  if (params.hydrationState) {
    return params.hydrationState;
  } else if (params.bucketIdTransformer) {
    return new BucketIdTransformerHydrationState(params.bucketIdTransformer);
  } else {
    return DEFAULT_HYDRATION_STATE;
  }
}
