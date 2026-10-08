# ADR 0006: WebSocket hints and HTTP recovery

Status: Accepted; HTTP recovery implemented and WebSocket hints deferred.

WebSocket new-head and log notifications wake a watcher but do not prove completeness or advance its durable checkpoint. HTTP `eth_getBlockByNumber`/`eth_getBlockByHash` and bounded `eth_getLogs` scans supply ordered catch-up. On startup, reconnect, suspicious log removal, or stale head, the watcher resumes from its committed checkpoint with an overlap window and verifies block hashes/ancestry. It splits rejected ranges and retries with backoff; an unresolved range blocks checkpoint advance.

This follows [Geth's subscription behavior](https://geth.ethereum.org/docs/interacting-with-geth/rpc/pubsub): subscriptions disappear with the connection and notifications are not historical replay. Missed WebSocket events therefore cannot create permanent gaps while HTTP remains available.

Both watchers use owned HTTP polling only. Each poll starts from its chain's committed checkpoint and processes a bounded contiguous unit. A later subscription component may wake that operation, but it may not decode a hint directly into durable checkpoint progress.
