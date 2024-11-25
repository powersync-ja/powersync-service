import { AbstractMigrationAgent, MigrationAgentGenerics, MigrationParams } from './AbstractMigrationAgent.js';
import * as defs from './migration-definitions.js';

export class MigrationManager<Generics extends MigrationAgentGenerics = MigrationAgentGenerics> {
  private migrations: defs.Migration<Generics['MIGRATION_CONTEXT']>[];
  private _agent: AbstractMigrationAgent<Generics> | null;

  constructor() {
    this.migrations = [];
    this._agent = null;
  }

  registerMigrationAgent(agent: AbstractMigrationAgent<Generics>) {
    if (this._agent) {
      throw new Error(`A migration agent has already been registered. Only a single agent is supported.`);
    }
    this._agent = agent;
  }

  registerMigrations(migrations: defs.Migration<Generics['MIGRATION_CONTEXT']>[]) {
    this.migrations.push(...migrations);
  }

  async migrate(params: MigrationParams) {
    if (!this._agent) {
      throw new Error(`A migration agent has not been registered yet.`);
    }
    return this._agent.run({
      ...params,
      migrations: this.migrations
    });
  }
}
