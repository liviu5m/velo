# Velo
Velo is a lightweight Redis-like server written in C using a single-threaded `poll` event loop and in-memory data structures.
It implements a subset of the Redis protocol (RESP) and command set, including strings, lists, streams, transactions (`MULTI/EXEC`), key watching, and basic master/replica replication flow.

## Project overview
This project provides:
- A TCP server (default port `6379`) that accepts RESP-formatted commands.
- In-memory storage for:
  - String keys (with optional expiration)
  - Lists
  - Streams
- Command execution engine with:
  - Core commands (`PING`, `ECHO`, `SET`, `GET`, `TYPE`, `INCR`, `INFO`)
  - Transactions (`WATCH`, `UNWATCH`, `MULTI`, `EXEC`, `DISCARD`)
  - List commands (`RPUSH`, `LPUSH`, `LLEN`, `LPOP`, `BLPOP`, `LRANGE`)
  - Stream commands (`XADD`, `XRANGE`, `XREAD`, `XREAD BLOCK`)
- Replication-related commands and handshake behavior (`REPLCONF`, `PSYNC`, propagation of writes).

## Main features and functionalities
## 1) RESP command parsing and command dispatch
- Parses RESP arrays into command arguments.
- Dispatches commands through dedicated modules:
  - `commands.c` for core + transactional + replication command handlers
  - `lists.c` for list operations
  - `streams.c` for stream operations

## 2) String key/value operations
- `SET key value [EX seconds | PX milliseconds]`
- `GET key`
- `TYPE key`
- `INCR key`
- Expiration tracked in milliseconds.

## 3) Transactions and optimistic locking
- `WATCH`/`UNWATCH` tracks keys per client session.
- `MULTI` queues commands.
- `EXEC` runs queued commands atomically from that queue perspective.
- `DISCARD` clears queued commands.
- If a watched key changes before `EXEC`, execution is aborted with null-array response.

## 4) List support
- `RPUSH`, `LPUSH`, `LLEN`, `LPOP`, `LRANGE`.
- `BLPOP` supports blocking pop with timeout handling.
- Blocked clients are checked on each event loop iteration.

## 5) Stream support
- `XADD` for stream inserts with generated or explicit IDs.
- `XRANGE` for range queries.
- `XREAD` with `STREAMS` and optional `BLOCK` mode.
- Blocked stream reads are resumed when new matching entries arrive or timeout occurs.

## 6) Replication flow (basic)
- Server can run as master (default) or replica (`--replicaof`).
- Replica startup performs:
  1. `PING`
  2. `REPLCONF listening-port`
  3. `REPLCONF capa psync2`
  4. `PSYNC ? -1`
- Master responds with `FULLRESYNC` and an empty RDB payload.
- Master propagates `SET` commands to connected replicas.

## 7) Connection model
- Single-process, single-threaded socket server.
- Uses `poll()` to multiplex server socket, client sockets, and master socket (when running as replica).
- Per-client state tracked in `clientSession`.

## Repository structure
```text
velo/
├── main.c                      # Server bootstrap, socket setup, poll loop, command execution path
├── commands.c / commands.h     # RESP parsing + core commands + transaction + replication handlers
├── lists.c / lists.h           # List commands and BLPOP timeout/unblock checks
├── streams.c / streams.h       # Stream commands and XREAD BLOCK processing
├── types.c / types.h           # Global data stores, session state, shared structures, time helper
├── velo                        # Tracked prebuilt Linux ELF executable
├── README.md
└── .vscode/                    # Local editor/debug configuration
```

## Supported commands
### Core
- `PING`
- `ECHO`
- `SET`
- `GET`
- `TYPE`
- `INCR`
- `INFO replication`

### Transactions / watch
- `WATCH`
- `UNWATCH`
- `MULTI`
- `EXEC`
- `DISCARD`

### Lists
- `RPUSH`
- `LPUSH`
- `LLEN`
- `LPOP`
- `BLPOP`
- `LRANGE`

### Streams
- `XADD`
- `XRANGE`
- `XREAD`

### Replication protocol helpers
- `REPLCONF`
- `PSYNC`

## Prerequisites
- Linux/macOS environment
- GCC (or compatible C compiler)
- `redis-cli` (recommended for interacting with the server)

## How to build
From the project root:
```bash
gcc -std=gnu11 -O2 -Wall -Wextra -pedantic main.c commands.c lists.c streams.c types.c -o velo
```

The repository already contains a built executable named `velo`, but rebuilding is recommended after code changes.

## How to run
## Run as master (default)
```bash
./velo
```
Runs on port `6379`.

## Run on a custom port
```bash
./velo --port 6380
```

## Run as replica
Start master first:
```bash
./velo --port 6379
```
Then start replica (note the quoted `host port` format expected by current argument parsing):
```bash
./velo --port 6380 --replicaof "127.0.0.1 6379"
```

## How to use
Use `redis-cli` against the port where Velo is running.

## Basic usage
```bash
redis-cli -p 6379 ping
redis-cli -p 6379 set foo bar
redis-cli -p 6379 get foo
redis-cli -p 6379 incr counter
redis-cli -p 6379 type foo
```

## Expiration examples
```bash
redis-cli -p 6379 set temp value EX 10
redis-cli -p 6379 set fast value PX 1500
```

## Transaction examples
```bash
redis-cli -p 6379 watch balance
redis-cli -p 6379 multi
redis-cli -p 6379 incr balance
redis-cli -p 6379 exec
```

## List examples
```bash
redis-cli -p 6379 rpush mylist a b c
redis-cli -p 6379 llen mylist
redis-cli -p 6379 lrange mylist 0 -1
redis-cli -p 6379 lpop mylist
redis-cli -p 6379 blpop mylist 5
```

## Stream examples
```bash
redis-cli -p 6379 xadd mystream * sensor-id 42 temp 21.5
redis-cli -p 6379 xrange mystream - +
redis-cli -p 6379 xread streams mystream 0-0
redis-cli -p 6379 xread block 5000 streams mystream $
```

## Design notes and current limitations
- Data is fully in-memory (no persistence to disk across restarts).
- Capacity is bounded by static arrays in code.
- RESP support is intentionally partial and focused on implemented commands.
- Replication support is simplified and currently propagates `SET` commands.
- Some behaviors are implementation-oriented rather than production-hardened (for learning/prototyping use).
