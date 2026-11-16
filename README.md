# tailpipe

A minimal pub/sub message broker, written from scratch in Go. It's my practice
ground for learning Go internals and systems-level programming - the first real
thing I've built outside the Java/Spring CRUD bubble.

## Why this exists

Five years of Java/Spring: JSON in, validate, map, REST out. Perfectly fine
work, but it stopped being interesting and I got burnt out on the repetition.
Also it teaches app-level programming, which again is super fine but just seems
boring to me, no hard feelings to what I've been doing and I still do for a living.
What actually pulls me is crypto infrastructure - indexers, order books, the
machinery running underneath the charts. That work is systems-level: raw bytes
on a socket, concurrency, memory, disk. None of which CRUD ever made me touch.

So this is the on-ramp. Not a library, not a product - a sandbox to break things
and learn. The goal was never "build a broker." It was "learn what a file handle
actually is, what a goroutine costs, why framing exists." The broker is just the
excuse.

## What it is

A NATS-flavoured pub/sub broker over raw TCP:

- length-prefixed binary framing - `[opcode][pattern-len][pattern][msg-len][msg]`
- publish / subscribe / unsubscribe, with `*` and `>` wildcard matching
- fan-out delivery and a non-blocking slow-consumer drop
- a write-ahead log for durability - fsync before ack
- group commit (batched fsync) and log rotation/retention to bound disk usage

The build went in phases, each one a lesson:

- **Phase 0** - TCP, sockets, framing, graceful shutdown. The stuff Tomcat had
  been doing for me all along.
- **Phase 1** - pub/sub, wildcards, and the slow-consumer problem.
- **Phase 2** - profiling and allocation reduction (pprof, benchstat).
- **Phase 3** - durability: WAL, group commit, rotation. The "a broker becomes a
  database" wall.

It's rough around the edges on purpose. This is training, not production.

## Run it

```bash
go run .               # listens on :4222
go test -race ./...    # tests + race detector
go test -bench=. -benchmem
```

## Where this is going

Project one of three:

1. **tailpipe** - this. Learn Go and systems fundamentals.
2. **reorgd** - a reorg-safe Ethereum indexer. The actual goal.
3. **bookworm** - an order book / cross-venue arbitrage engine.

Each one builds on the last. tailpipe is the one just kicking things off.
