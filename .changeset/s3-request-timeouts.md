---
'@powersync/service-module-mongodb-storage': minor
---

Apply request timeouts to S3 object storage. The AWS SDK does not use any timeouts by default, so a stalled request could hold on to one of the limited S3 operation slots indefinitely, eventually blocking all reads. Timeouts are derived from the AWS defaults mode (`AWS_DEFAULTS_MODE`, `defaults_mode` in the AWS shared config file, or the new `storage.object_storage.defaults_mode` option), instead of being individually configurable. The AWS default of `legacy` defines no timeouts of its own, and uses the same baseline as `standard`.
