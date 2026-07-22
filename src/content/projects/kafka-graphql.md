---
title: 'Kafka × GraphQL — Live Order Tracking'
description: 'A real-time demo pairing Confluent Cloud (Kafka + Schema Registry) with GraphQL subscriptions to stream order status to the browser as it happens.'
pubDate: 2026-07-22
tags: ['Python', 'Kafka', 'GraphQL', 'Confluent', 'Avro', 'Real-time']
status: 'completed'
repo: 'https://github.com/MosheBlumbergX/kafkaGraphQL'
image: '/kafka-graphql.svg'
---

![Live order-tracking cards moving through PLACED, PREPARING, SHIPPED and DELIVERED](/kafka-graphql.svg)

## Overview

You click **Place order** and a card appears saying *PLACED*. You touch nothing
else — and a few seconds later it quietly changes to *PREPARING*, then *SHIPPED*,
then *DELIVERED*. No refresh, no polling. It just moves.

This project is the story of what happens in those few seconds, and why pairing
**Kafka** with **GraphQL** is what makes that little card feel alive. Kafka does
the work and remembers everything; GraphQL is the friendly doorway that lets a
browser *place* orders, *ask* about them, and — the star of the show — *watch
them update live* over a WebSocket, without ever needing to know Kafka exists.

## How it works

An order flows through the system like a ticket through a restaurant:

- **The mutation** (`placeOrder`) drops a ticket onto a Kafka `orders` topic.
- **A processor** consumes it and emits status transitions
  (PLACED → PREPARING → SHIPPED → DELIVERED) to an `order-events` topic.
- **The GraphQL server** tails `order-events` on a background thread and fans each
  change out to (a) an in-memory read model for queries and (b) a live
  **subscription** for every connected browser.

```
placeOrder ──► [orders] ──► processor ──► [order-events]
                                               │
 browser ◄── orderStatus subscription ◄── GraphQL server consumes order-events
```

## What it demonstrates

- **GraphQL subscriptions over Kafka** — the natural pairing: Kafka is a live
  river of events, and a subscription pipes it straight to the screen.
- **Rebuild-from-the-log** — the server keeps no state on disk. Restart it and it
  replays the topic from offset 0, rebuilding its entire view of the world. The
  log is the source of truth; the UI is a disposable projection (event
  sourcing / CQRS).
- **Decoupled producers and consumers** — a standalone script can place orders
  straight onto Kafka while the web server is *offline*; when it comes back, every
  order is already there, processed. Proof that GraphQL is one door into the
  system, not the system itself.

## Technical details

- **Kafka + Schema Registry:** Confluent Cloud, with **Avro** serialization and
  auto-registered schemas so producers and consumers agree on the message format.
- **GraphQL server:** Python + Strawberry on Starlette/uvicorn, serving both the
  API and the web page; queries, a mutation, and a WebSocket subscription.
- **Kafka client:** `confluent-kafka` (librdkafka). Because it's blocking, the
  consumer runs on a background thread and bridges to asyncio via
  `loop.call_soon_threadsafe` — the trickiest and most interesting piece.
- **Frontend:** a zero-build React page (loaded from a CDN) with self-updating
  order cards, speaking the `graphql-transport-ws` protocol directly.

## Challenges & lessons

1. **Blocking client meets async server.** librdkafka's `poll()` blocks, but
   GraphQL subscriptions are asyncio generators. Solved with a dedicated consumer
   thread and a thread-safe hop onto the event loop.
2. **WebSockets can't follow redirects.** Mounting the GraphQL app at `/graphql`
   made Starlette 307-redirect to `/graphql/`, which silently 403'd the
   subscription socket. Fixed by serving it with an explicit `Route` +
   `WebSocketRoute` at the exact path.
3. **"Rebuild from the log" only works if you don't commit offsets.** The obvious
   default (auto-commit) makes a restarted consumer resume instead of replay —
   the opposite of what a rebuildable read model needs.
