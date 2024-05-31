declare module '@syncpoint/wkx' {
  export class Geometry {
    static parse(blob: any): Geometry | null;
    toGeoJSON(): any;
    toWkt(): string;
  }

  export class Point extends Geometry {
    x: number;
    y: number;
  }
}
