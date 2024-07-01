export type ProbeState = {
  ready: boolean;
  started: boolean;
  touched_at: Date;
};

export type ProbeModule = {
  poll_timeout: number;

  state(): ProbeState;

  ready(): Promise<void>;
  unready(): Promise<void>;
  touch(): Promise<void>;
};
