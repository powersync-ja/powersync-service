---
'@powersync/service-module-mssql': minor
---

Add custom CA certificate support for MSSQL connections.

The new `cacert` option allows PowerSync to validate SQL Server certificates issued by a private CA
without enabling `trustServerCertificate`. An optional `tls_servername` can be specified when the connection
hostname differs from the name in the server certificate.
