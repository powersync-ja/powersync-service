export type Migration = {
  name: string;
  up: () => Promise<void>;
  down: () => Promise<void>;
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
