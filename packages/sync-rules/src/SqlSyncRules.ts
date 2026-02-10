import { BucketParameterQuerier, QuerierError } from './BucketParameterQuerier.js';
import { SyncRulesErrors, YamlError } from './errors.js';
import { TablePattern } from './TablePattern.js';
import { RequestParameters, SourceSchema, SqliteJsonRow } from './types.js';
import { SyncConfig, SyncConfigWithErrors } from './SyncConfig.js';
import { SyncConfigFromYaml } from './from_yaml.js';

export interface SyncRulesOptions {
  schema?: SourceSchema;
  /**
   * The default schema to use when only a table name is specified.
   *
   * 'public' for Postgres, default database for MongoDB/MySQL.
   */
  defaultSchema: string;

  throwOnError?: boolean;
}

export interface RequestedStream {
  /**
   * The parameters for the explicit stream subscription.
   *
   * Unlike {@link GetQuerierOptions.globalParameters}, these parameters are only applied to the particular stream.
   */
  parameters: SqliteJsonRow | null;

  /**
   * An opaque id of the stream subscription, used to associate buckets with the stream subscriptions that have caused
   * them to be included.
   */
  opaque_id: number;
}

export interface GetQuerierOptions {
  globalParameters: RequestParameters;
  /**
   * Whether the client is subscribing to default query streams.
   *
   * Client do this by default, but can disable the behavior if needed.
   */
  hasDefaultStreams: boolean;
  /**
   *
   * For streams, this is invoked to check whether the client has opened the relevant stream.
   *
   * @param name The name of the stream as it appears in the sync rule definitions.
   * @returns If the strema has been opened by the client, the stream parameters for that particular stream. Otherwise
   * null.
   */
  streams: Record<string, RequestedStream[]>;
}

export interface GetBucketParameterQuerierResult {
  querier: BucketParameterQuerier;
  errors: QuerierError[];
}

export class SqlSyncRules extends SyncConfig {
  static validate(yaml: string, options: SyncRulesOptions): YamlError[] {
    try {
      const rules = this.fromYaml(yaml, options);
      return rules.errors;
    } catch (e) {
      if (e instanceof SyncRulesErrors) {
        return e.errors;
      } else if (e instanceof YamlError) {
        return [e];
      } else {
        return [new YamlError(e)];
      }
    }
  }

  static fromYaml(yaml: string, options: SyncRulesOptions): SyncConfigWithErrors {
    const parser = new SyncConfigFromYaml(
      {
        throwOnError: options.throwOnError ?? true,
        schema: options.schema,
        defaultSchema: options.defaultSchema
      },
      yaml
    );

    return parser.read();
  }
}
