## Legacy Sync Rules and Sync Streams alpha

Sources in this directory implement Legacy Sync Rules (as a format with explicit parameter and data queries)
and an outdated alpha version of Sync Streams.

We must preserve this implementation for backwards-compatibility, as new service versions should support existing
deployments using those features.
We will deprecate and remove them in a future major revision of the PowerSync Service.

No new features should be added here, the current implementation of Sync Streams is in `../compiler/` and
`../sync_plan/`.
