---
title: 'GC Lab: Heap, OOM, and Zombie JVMs'
description: 'A Java 21 Maven lab that reproduces heap, Metaspace, and direct-buffer OOM plus two zombie process modes, then maps each onto Kafka brokers and clients.'
pubDate: 2026-09-22
tags: ['Java', 'JVM', 'G1GC', 'Kafka', 'Maven']
status: 'completed'
repo: 'https://github.com/MosheBlumbergX/garbageCollection'
image: '/gc-lab.svg'
---

![Healthy Eden, three OOM paths, and two zombie JVM modes](/gc-lab.svg)

## Overview

You ssh to a Kafka broker because produce p99 just exploded. `jps` still
lists the process. A heartbeat thread is printing `UP`. Clients are timing
out. ISR is shrinking.

That is a **zombie JVM**: the PID is alive, the work is dead. Brokers,
Connect, Streams, and the Java clients are ordinary HotSpot processes. They
fail the same ways a small Java app fails, then the cluster notices (session
timeout, missed poll, fenced controller).

This project is a Java 21 Maven app that makes those shapes loud on purpose.
Each scenario is a main-arg plus a script with the JVM flags that trigger it.
There is no Kafka cluster. The `kafka-style` scenario is in-process: a producer
accumulator, a stuck consumer poll, and a broker thread that retains request
copies.

Think of the heap like a kitchen counter. Short-lived produce/fetch payloads
are dishes you wash as you go (Eden). A leak is every plate stacked in the
sink until you cannot move. Metaspace and direct buffers are the pantry and
the freezer: they are not on the counter (`-Xmx`), and they can still fill
the house.

## How it works

Seven isolated mains, one failure mode each. Prefer the scripts so the flags
match the story:

- **`healthy`** allocates 64KiB arrays and drops them. Young GC reclaims
  Eden. Heap stays flat. This is the allocation shape you want on a Kafka
  request path.
- **`heap-oom` / `metaspace-oom` / `direct-oom`** retain every byte, every
  class-loader, or every direct `ByteBuffer` until that pool throws.
- **`zombie-gc`** fills the heap and keeps allocating. G1 thrashes. CPU is
  the collector. Heartbeats still print.
- **`zombie-oom`** catches `OutOfMemoryError` and keeps the process up.
  Heartbeats stay green. Serving does not resume.
- **`kafka-style`** walks three phases: full `buffer.memory` analog, a
  consumer that misses `max.poll.interval.ms`, then a handler that retains
  payloads until heap pressure.

```
new object ──► Eden ──► still reachable?
                  │            │
                  │            no ──► reclaimed   (healthy)
                  │            yes
                  ▼
             Survivor ──► Old ──► heap OOM  /  GC thrash
                  │
                  ├── class metadata ─────────► Metaspace OOM
                  └── ByteBuffer.allocateDirect ► Direct buffer OOM
```

`-Xmx` only caps the Java heap. Kafka dies from the other three as well:
Metaspace (Connect class-loaders), direct NIO buffers, and OS page cache
starved by a heap that ate the box.

In a second terminal, while any scenario is running:

```bash
jps -l
./scripts/monitor.sh <pid>    # jstat -gcutil, one-second samples
```

GC logs land in `logs/`. Heap dumps land in `dumps/` when the JVM actually
OOMs.

## What it demonstrates

- **Young GC doing its job.** Eden sawtooth, old gen flat, process stays up.
  That is `run-healthy.sh`.
- **Three different `OutOfMemoryError` messages.** `Java heap space`,
  `Metaspace`, and `Direct buffer memory`. Graph only heap and you will miss
  two of them.
- **Two zombies, on purpose.** GC thrash: the JVM never (or only eventually)
  throws; useful request/s collapse. Survived OOM: someone caught `Error`;
  the PID is up; the heap is wrecked. The second one is the dangerous case
  on a broker.
- **Kafka names on the same failures.** Heap vs page cache, ISR shrinks
  when pauses exceed `replica.lag.time.max.ms`, controller / KRaft / ZK
  session fence, producer `buffer.memory`, consumer
  `max.poll.interval.ms` rebalance, Connect plugin loaders, direct
  `ByteBuffer`s for NIO. No live cluster required.

| Script | Kafka story |
|---|---|
| `run-healthy.sh` | Request payload dies in Eden |
| `run-heap-oom.sh` | Unbounded retained records / leaky interceptor |
| `run-metaspace-oom.sh` | Connect plugin / custom Serde class-loader leak |
| `run-direct-oom.sh` | NIO / Netty buffers unbounded; `-Xmx` looks fine |
| `run-zombie-gc.sh` | Broker up, produce p99 terrible, ISR flaps |
| `run-zombie-oom.sh` | Someone caught `Error`; process still in the group |
| `run-kafka-style.sh` | Full accumulator, missed poll, broker copies payloads |

## Technical details

- **Stack:** Java 21, Maven (`gc-lab` jar), G1 pinned so the GC log format
  stays consistent. Scenarios live under
  `src/main/java/com/gcdemo/scenarios/`. `MemoryReporter` prints heap,
  pools, GC, and direct buffers. `EmptyClassBytes` emits tiny class files
  for the Metaspace leak.
- **Flags that match the story.** OOM scripts set
  `-XX:+HeapDumpOnOutOfMemoryError` and `-XX:+ExitOnOutOfMemoryError`.
  `zombie-gc` turns **off** `UseGCOverheadLimit` so you can watch thrash.
  `zombie-oom` omits both dump-and-exit flags on purpose: a heap dump would
  consume the last of the heap and kill the zombie before you can see it
  limp. Production brokers should set both.
- **Docs, not a cluster.** `docs/01` through `05` cover generations,
  expected output, `jstat` / `jcmd` / JFR, production gotchas, and the
  Kafka mapping. The lab never starts a broker.
- **Honest constraint.** This is a teaching JVM, not a load test. Heaps
  are tens of MiB so failures happen in seconds. The shapes are the same
  as a 6–8 GiB broker heap; the clock is faster.

## Challenges & lessons

1. **Catching `OutOfMemoryError` manufactures a zombie.** It is an
   `Error`. After it is thrown the heap is inconsistent: caches half-built,
   buffers half-filled. Catching it in one thread does not free memory.
   Prefer `-XX:+ExitOnOutOfMemoryError` on brokers and let the supervisor
   move leadership.

2. **`-Xmx` is not how much RAM the process uses.** RSS is heap plus
   Metaspace, direct, stacks, mmap, and allocator slop. Kafka mmap's index
   files. Raising the heap to "use the machine" starves OS page cache
   (the real broker cache) and the box goes disk-bound.

3. **G1 can thrash forever without an OOM.** GC overhead limit is not a
   reliable backstop on G1. Alert on GC time ratio, pause p99, and the
   Kafka SLI (`RequestHandlerAvgIdlePercent`, produce/fetch p99, ISR), not
   only on the OOM log line.

4. **A green heartbeat is not a health check.** The heartbeat thread can
   stay alive while the worker is in GC or in a catch-and-continue. A
   liveness probe that only checks a TCP port restarts a zombie late or
   never. Check a real SLI.

5. **`max.poll.interval.ms` is a GC plus processing budget.** A 6s stall
   with a 5s interval prints `REBALANCE`. The consumer JVM never crashed.
   The group coordinator did its job. Same event as a 6s full GC.

6. **Direct memory and Metaspace are invisible to a heap-only graph.**
   `ByteBuffer.allocateDirect` and a retained `ClassLoader` do not show up
   as `Java heap space`. Cap `MaxDirectMemorySize` and `MaxMetaspaceSize`,
   export `java.nio.BufferPool`, and graph class count after warmup.
