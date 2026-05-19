---
'@powersync/lib-services-framework': patch
---

Normalize host strings in `validateIpHostname` before IP validation. Adds `hostWithoutPort`, which strips port suffixes and unwraps bracketed IPv6 literals, so values produced by `URL.hostname` and the `uri.hosts` entries returned by `mongodb-connection-string-url` are recognized as IPs.
