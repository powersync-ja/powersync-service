---
'@powersync/service-module-mongodb-storage': minor
---

Apply timeouts to S3 object storage. The AWS SDK does not use any timeouts by default, so a stalled request could hold on to one of the limited S3 operation slots indefinitely, eventually blocking all reads. Requests are now bounded per attempt and per operation, and waiting for an operation slot is bounded as well. The timeouts are derived from the AWS defaults mode (the new `storage.object_storage.defaults_mode` option, or the `AWS_DEFAULTS_MODE` environment variable), instead of being individually configurable. Unlike the AWS SDK, `defaults_mode` in the AWS shared config file is not consulted, since the mode is resolved synchronously on startup. The AWS defaults of `legacy` and `auto` both use the same baseline as `standard`: `legacy` defines no timeouts of its own, and `auto` is not used because it makes the timeouts depend on the deployment environment, detected by querying the EC2 instance metadata service on startup.
