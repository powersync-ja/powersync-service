export type MigrationFunction<Context extends {} | undefined = undefined> = (context: Context) => Promise<void>;

export type Migration<Context extends {} | undefined = undefined> = {
  name: string;
  up: MigrationFunction<Context>;
  down: MigrationFunction<Context>;
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
