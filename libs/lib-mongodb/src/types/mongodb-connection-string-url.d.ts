declare module 'mongodb-connection-string-url' {
  export class ConnectionURI {
    constructor(uri: string);
    toString(): string;
    searchParams: URLSearchParams;
    pathname: string;
    username: string;
    password: string;
    hosts: string[];
    isSRV: boolean;
    href: string;
  }
  export default ConnectionURI;
}
