export type Migration<Context extends {} | undefined = undefined> = {
  name: string;
  up: (context: Context) => Promise<void>;
  down: (context: Context) => Promise<void>;
};

export enum Direction {
  Up = 'up',
  Down = 'down'
}

export type ExecutedMigration = {
  name: string;
  direction: Direction;
  timestamp: Date;
};

export type MigrationState = {
  last_run: string;
  log: ExecutedMigration[];
};

export type MigrationStore = {
  load: () => Promise<MigrationState | undefined>;
  save: (state: MigrationState) => Promise<void>;
};
