---
'@powersync/service-module-mysql': minor
---

Added a configurable limit for the MySQL binlog processing queue to limit memory usage.
Removed MySQL Zongji type definitions, they are now instead imported from the `@powersync/mysql-zongji` package.
Now passing in port for the Zongji connection, so that it can be used with MySQL servers that are not running on the default port 3306.
