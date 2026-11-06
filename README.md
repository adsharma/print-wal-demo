# print_wal — litestream-style WAL tailer for Ladybug

A Ladybug `EXTENSION` extension that keeps a **long-running read-only transaction**
open to prevent the WAL from being cleaned up, while a **publisher thread tails
the WAL file and publishes (prints) each record**.

## Technique

- `CALL print_wal_start()` opens a dedicated `Connection` on the same
  `Database` and begins a read-only transaction on it that is never committed
  until `CALL print_wal_stop()`. The checkpointer derives a *snapshot horizon*
  from all open transactions (`TransactionManager::snapshotHorizonInfo`); a
  non-empty horizon makes `Checkpointer::beginCheckpoint` skip WAL rotation and
  `postCheckpointCleanup` skip WAL retirement. The WAL file therefore grows
  append-only instead of being truncated/reset — the same pinning idea
  litestream uses on SQLite.
- The publisher thread polls the WAL file size, re-opens it, and deserializes
  records with `WALRecord::deserialize` (honouring the DB checksum setting via
  `ChecksumReader`/`BufferedFileReader`). It tracks the next byte offset so
  each committed record is published exactly once; torn tails are left for the
  next poll. Output goes to stdout as `[print_wal] offset=... type=...`.

## Layout

- `print_wal.cpp` — the extension (`PRINT_WAL_START` / `PRINT_WAL_STATUS` /
  `PRINT_WAL_STOP` standalone table functions).
- `demo.cpp` — creates a database, loads the extension, then writes DDL+DML.
- `CMakeLists.txt` — standalone build (headers from the repo, no repo changes).

## Build & run

```bash
cmake -B /tmp/print-wal-demo/build -S /tmp/print-wal-demo \
    -DLBUG_REPO=/home/ubuntu/src/ladybug
cmake --build /tmp/print-wal-demo/build -j
/tmp/print-wal-demo/build/demo
```

To point at another checkout: `-DLBUG_REPO=<path>` (needs
`<repo>/build/release/src/liblbug.so` — run `make release` there first).

## Notes

- Requires an on-disk database (`:memory:` has no WAL file).
- Order matters: call `CALL print-wal-demo_stop()` before destroying the `Database`.
- While pinned, the WAL file grows without bound — this is a demo of the
  technique, not a production replication feed.
