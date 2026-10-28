export class ReplicationChildClassFixture {
  constructor(
    readonly name: string,
    readonly count: number
  ) {}
}

export const notConstructible = { kind: 'plain-object' };
