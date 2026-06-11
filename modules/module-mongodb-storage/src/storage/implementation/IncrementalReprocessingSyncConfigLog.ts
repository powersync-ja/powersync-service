import {
  BucketDataSource,
  ParameterIndexLookupCreator,
  SyncConfigWithErrors,
  TablePattern
} from '@powersync/service-sync-rules';
import {
  IncrementalMappingChanges,
  IncrementalMappingDefinitionChange,
  SingleSyncConfigBucketDefinitionMapping
} from './BucketDefinitionMapping.js';

export interface IncrementalDefinitionLogEntry extends IncrementalMappingDefinitionChange {
  sourceTables?: string[];
  snapshotTables?: string[];
}

export interface IncrementalSyncConfigUpdateLog {
  reusedDefinitions: IncrementalDefinitionLogEntry[];
  addedDefinitions: IncrementalDefinitionLogEntry[];
  droppedDefinitions: IncrementalDefinitionLogEntry[];
}

export function formatIncrementalSyncConfigUpdateLog(log: IncrementalSyncConfigUpdateLog) {
  return [
    formatDefinitionSection('Reused definitions', log.reusedDefinitions),
    formatDefinitionSection('New definitions', log.addedDefinitions, { includeSnapshotTables: true }),
    formatDefinitionSection('Definitions to drop after switching', log.droppedDefinitions)
  ].join('\n');
}

export function describeIncrementalSyncConfigUpdate(options: {
  activeMappings: SingleSyncConfigBucketDefinitionMapping[];
  newMapping: SingleSyncConfigBucketDefinitionMapping;
  newSyncConfig: SyncConfigWithErrors;
  mappingChanges: IncrementalMappingChanges;
}): IncrementalSyncConfigUpdateLog {
  const { activeMappings, newMapping, newSyncConfig, mappingChanges } = options;
  const newDefinitionKeys = new Set(newMapping.allDefinitionEntries().map(definitionKey));
  const activeDefinitions = uniqueDefinitions(activeMappings.flatMap((mapping) => mapping.allDefinitionEntries()));

  return {
    reusedDefinitions: mappingChanges.reusedDefinitions,
    addedDefinitions: mappingChanges.addedDefinitions.map((definition) => {
      const sourceTables = sourceTablesForDefinition(newSyncConfig, newMapping, definition);
      return {
        ...definition,
        sourceTables,
        snapshotTables: sourceTables
      };
    }),
    droppedDefinitions: activeDefinitions.filter((definition) => !newDefinitionKeys.has(definitionKey(definition)))
  };
}

function sourceTablesForDefinition(
  syncConfig: SyncConfigWithErrors,
  mapping: SingleSyncConfigBucketDefinitionMapping,
  definition: IncrementalMappingDefinitionChange
) {
  if (definition.type == 'bucket_data') {
    return sourceTablesForSources(
      syncConfig.config.bucketDataSources.filter((source) => mapping.bucketSourceId(source) == definition.id)
    );
  }

  return sourceTablesForSources(
    syncConfig.config.bucketParameterLookupSources.filter(
      (source) => mapping.parameterLookupId(source) == definition.id
    )
  );
}

function sourceTablesForSources(sources: Array<BucketDataSource | ParameterIndexLookupCreator>) {
  return uniqueSorted(
    sources.flatMap((source) => [...source.getSourceTables()].map((table) => formatTablePattern(table)))
  );
}

function definitionKey(definition: IncrementalMappingDefinitionChange) {
  return `${definition.type}:${definition.id}`;
}

function uniqueSorted(values: string[]) {
  return [...new Set(values)].sort();
}

function uniqueDefinitions(definitions: IncrementalMappingDefinitionChange[]) {
  const byKey = new Map<string, IncrementalMappingDefinitionChange>();
  for (const definition of definitions) {
    byKey.set(definitionKey(definition), definition);
  }
  return [...byKey.values()];
}

function formatTablePattern(pattern: TablePattern) {
  return `${pattern.connectionTag}.${pattern.schema}.${pattern.tablePattern}`;
}

function formatDefinitionSection(
  title: string,
  definitions: IncrementalDefinitionLogEntry[],
  options: { includeSnapshotTables?: boolean } = {}
) {
  if (definitions.length == 0) {
    return `${title}: none`;
  }

  return [`${title}:`, ...definitions.map((definition) => `  - ${formatDefinition(definition, options)}`)].join('\n');
}

function formatDefinition(definition: IncrementalDefinitionLogEntry, options: { includeSnapshotTables?: boolean }) {
  const details = [`type=${definition.type}`, `id=${definition.id}`, `name=${definition.name}`];
  if (definition.sourceTables != null) {
    details.push(`tables=${formatList(definition.sourceTables)}`);
  }
  if (options.includeSnapshotTables && definition.snapshotTables != null) {
    details.push(`re-snapshot=${formatList(definition.snapshotTables)}`);
  }
  return details.join(', ');
}

function formatList(values: string[]) {
  return values.length == 0 ? 'none' : values.join(', ');
}
