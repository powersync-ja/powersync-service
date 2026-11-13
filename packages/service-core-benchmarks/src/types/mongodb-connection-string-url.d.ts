declare module 'mongodb-connection-string-url' {
  export class ConnectionURI {
    constructor(uri: string);
    toString(): string;
    hosts: string[];
    isSRV: boolean;
    href: string;
  }
  export default ConnectionURI;
}
