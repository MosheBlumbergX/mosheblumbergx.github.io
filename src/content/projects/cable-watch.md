---
title: 'Cable Watch: Dark Vessels over Subsea Cables'
description: 'A Terraform demo that streams live Baltic AIS through Confluent Cloud, where Flink SQL scores ships loitering, dragging anchor or going dark over 47 subsea cables and pipelines and an HTTP Sink sends the alert to your phone.'
pubDate: 2026-10-20
tags: ['Kafka', 'Flink', 'Confluent', 'Python', 'Terraform', 'AWS']
status: 'completed'
repo: 'https://github.com/MosheBlumbergX/cableWatch'
image: '/cable-watch.svg'
---

![Live AIS through Kafka and Flink SQL detection to a risk score, an incident and a phone alert](/cable-watch.svg)

## Overview

You read the news after a Baltic cable goes dark: Balticconnector in 2023,
C-Lion1 and BCS East-West in 2024, Estlink 2 a month later. Then you read that
the suspect ship's behaviour was there all along in public AIS data. It crept
slowly over the corridor, dragged an anchor for tens of kilometres, and left
gaps in its track. Nobody was watching at the time.

The catch is that none of those signals lives in a single position report. One
AIS message is a ship, a place and a speed. "Slow for twenty minutes over a
cable" or "silent for half an hour near one" only exists across time, per
vessel, against a fixed map. It is like a security guard who sees one frame of
CCTV at a time: a person standing in a doorway means nothing until you notice
they have been there for an hour.

This project gives that guard a memory. A Python producer reads the
aisstream.io WebSocket over the Baltic, about 12 reports a second from roughly
3,000 vessels, into Kafka on Confluent Cloud. Flink SQL detects loitering,
anchor drag and going dark over 47 cables and pipelines, turns them into a
decaying risk score per vessel and cable, and raises WATCH and ALERT
incidents. A fully managed HTTP Sink connector posts the alert to ntfy.sh, so
your phone buzzes. No human and no custom consumer sit between the ship's
radio and the notification.

![The dashboard: the Baltic as a nautical chart with magenta cables and pipelines, every vessel the feed hears, an incident log on the left with a dredger on ALERT over the Bornholm-Germany interconnector, and a pipeline strip along the bottom with the live rate of every topic](/cable-watch-dashboard.jpg)

## How it works

One `make infra` builds a Confluent Cloud environment (Kafka, Schema Registry
with Stream Governance, a Flink pool, HTTP Sink connectors, Tableflow) and an
AWS `t4g.small` behind CloudFront that loads the cable map, runs the producer
and serves the dashboard. The detection is all SQL, numbered in pipeline order.

- **Geofence as a join.** The producer tags each fix with its H3 cell. A
  compacted `cable_cells` topic maps 27,131 cells to the cables within 10 km,
  so one event-time temporal join replaces any point-in-polygon work.
- **Loitering.** A 20-minute tumbling window per vessel and cable over fixes
  under 2 knots within 2 km of a route.
- **Anchor drag.** `MATCH_RECOGNIZE` over eight consecutive corridor fixes at
  1.5 to 7 knots, either reporting "at anchor" or moving more than 45° off the
  bow. That is the Eagle S signature.
- **Going dark.** A 30-minute session window per vessel. The watermark comes
  from every other ship in the Baltic, so a silent vessel still gets a clock.
  An interval join fetches its last fix and a temporal join checks it was
  within 10 km of a cable.
- **Score and incidents.** An `OVER` window sums the last hour of points per
  vessel and cable: under 30 is NORMAL, 30 to 59 WATCH, 60 and over ALERT. A
  one-hour session window emits the CLEAR that resolves the incident.
- **Outcome.** Payload statements write the exact JSON each HTTP Sink posts,
  to ntfy.sh and, optionally, PagerDuty and Slack. With an OpenAI key,
  `ML_PREDICT` rewrites each incident as a two-sentence brief.

```
aisstream.io ──► producer (Avro, H3, data contract) ──► [ais_position_raw]
                                                              │
OpenStreetMap ──► [cable_cells] ──► Flink SQL: loiter · drag · dark
                                              │
                                    [risk_updates] ──► [incidents]
                                              │
                      HTTP Sink ──► ntfy · PagerDuty · Slack
                      FastAPI SSE ──► MapLibre dashboard
```

## What it demonstrates

- **Behaviour first, identity second.** A watch list adds 15 points per
  reason: a flag tied to past incidents or the "shadow fleet," a tanker, a
  cargo ship with no destination, or a repeat offender with 3 or more signals
  in 7 days. It only applies on top of a detection, so no ship is raised for
  who it is. A listed tanker that goes dark near a cable scores 40 + 15 + 15 =
  70 and goes straight to ALERT.
- **A vessel you can read.** Click a ship and the panel says what it is, built
  from AIS static data ("a dredger sailing under the flag of Denmark"), with
  its speed sparkline, its score per cable, and every signal that scored.
- **A pipeline you can watch.** The strip along the bottom shows every topic
  from raw AIS to the ntfy DLQ with its live rate or total. Stream Lineage
  shows the same graph inside Confluent Cloud.
- **Governance in the path.** CEL rules on `ais_position_raw` send impossible
  fixes (latitude 91 means "not available") to a DLQ from inside the
  producer's serializer, with `__rule.*` headers naming the broken rule.

![The vessel panel for the dredger SANDBORN: what kind of ship it is, its flag, call sign, destination and draught, a speed sparkline, its risk score against the Bornholm-Germany interconnector, and the loitering signals that scored](/cable-watch-vessel-panel.jpg)

## Technical details

- **Stack:** a Python producer, cable loader and FastAPI dashboard (MapLibre,
  server-sent events) on one EC2 instance; Confluent Cloud Kafka, Schema
  Registry (Avro), Flink SQL, HTTP Sink V2 and Tableflow; Terraform with the
  `confluentinc/confluent` provider for both clouds.
- **Schemas live in Flink.** `CREATE TABLE` registers each Avro schema. The
  producer and loader write with them and never register their own.
- **The map is committed.** `tools/build_cables.py` pulls every named
  submarine cable and pipeline from OpenStreetMap, buffers it by 10 km, fills
  the buffer with H3 resolution-7 cells and drops cells within 3 km of land.
  The output is in the repo, so a deploy never depends on Overpass.
- **Iceberg on the side.** Tableflow materialises `vessel_fixes`,
  `vessel_risk` and `incidents` as Iceberg tables.
- **Honest limits.** aisstream.io coverage comes from volunteer receivers: dense in the Gulf of
  Finland and the Danish straits, sparse in the central Baltic. "Gone silent"
  means this feed stopped hearing the ship, not that its transponder is off.

## Challenges & lessons

1. **Static reference data needs a far-future watermark.** `cable_cells` was
   written with 2020 timestamps, and a temporal join waits for its slower
   input, so a reload froze the whole pipeline overnight. Its watermark now
   sits a century ahead, which is only safe with idleness detection off;
   otherwise the join jumps to 2119 and drops every fix as late.
2. **Sparse topics need a heartbeat.** Detections are rare and an event-time
   operator only emits when a later row moves its watermark. `risk_signals`
   carries a heartbeat row every 30 seconds from a window over
   `vessel_fixes`.
3. **One writer per topic.** The running score and the decay were once two
   statements writing `risk_updates`. Their transactions committed at
   different checkpoints and the incident statement dropped the "late" rows.
   They now run as one `UNION ALL` statement.
4. **`LAG` is unsafe with ties.** It uses a RANGE frame, so two updates with
   the same timestamp see each other. Transitions use a two-row `ROWS` frame
   with `FIRST_VALUE` instead.
5. **Service vessels are the loudest signal.** On the first day of live data,
   three Helsinki pilot boats raised 27 of 31 incidents. Pilots, tugs, rescue
   and police vessels are still scored, but get no watch-list points, no
   notifications, and stay hidden on the map unless you ask for them.
6. **A free feed has coverage holes.** A cargo ship at 7.5 knots in the
   Kattegat was flagged dark while MarineTraffic still saw it. Only vessels
   under 3 knots when last heard can go dark, because a ship under way has
   almost always just sailed out of receiver range.
