import { NodeLocation, PGNode } from 'pgsql-ast-parser';

export interface ParsingErrorListener {
  report(message: string, location: NodeLocation | PGNode): void;
}
