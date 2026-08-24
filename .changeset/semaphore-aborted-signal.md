---
'@powersync/service-core': patch
---

Fix `acquireSemaphoreAbortable()` waiting indefinitely when passed a signal that was already aborted. An already-aborted signal never fires its `abort` listener, so the call waited for the semaphore instead of returning immediately.
