---
title: 'ISS Live: Kafka and Flink on a Globe'
description: 'A Terraform demo on AWS that streams ISS fixes and altitude-matched satellites through Confluent Cloud and Flink SQL onto a 3D globe.'
pubDate: 2026-10-19
updatedDate: 2026-10-19
tags: ['Kafka', 'Flink', 'Confluent', 'Python', 'Terraform', 'AWS']
status: 'completed'
repo: 'https://github.com/MosheBlumbergX/issDemo'
image: '/iss-demo.svg'
---

![ISS fixes and satellite positions through Kafka and Flink SQL onto a 3D globe](/iss-demo.svg)

## Overview

You look up and the station is a bright dot, gone again in a couple of minutes.
One position fix is only a latitude, a longitude, an altitude, and a time. Ask
which way it is heading, whether it just crossed into night, or whether another
satellite is about to pass 30 km away, and that single message has no answer.
Those questions live in the gap between this fix and the last one.

This project puts that gap in **Flink**. Once a second, a Python producer on
EC2 writes the ISS position to Kafka on Confluent Cloud. A second producer
writes every satellite whose orbit reaches the station's altitude, about 6,000
of them, mostly Starlink. Sixteen Flink SQL statements turn the two streams
into speed, heading, city flyovers, measured orbit periods, and close
approaches. A dashboard on the same host pushes the results to a 3D globe with
server-sent events.

![The dashboard: a 3D globe with the ISS over the southern Indian Ocean inside a shell of faint satellite dots, its orbit trail and predicted path. Live telemetry is on the left; on the right, the nearest satellite right now, the next predicted close approaches and a flight log of Flink-detected events](/iss-demo-dashboard.jpg)

## How it works

One `terraform apply` builds the Confluent environment (Kafka, Schema Registry,
a Flink pool) and the AWS host that serves the page. The SQL is the product:
each statement relates messages over time.

- **Enrich.** `LAG` compares each ISS fix with the previous one. Haversine
  distance becomes ground-track speed. Great-circle bearing becomes heading.
- **Cities.** Every fix is fanned across 158 cities with `UNNEST`. The closest
  approach is the middle of three distances carried on one row.
- **Orbits and distance.** `MATCH_RECOGNIZE` measures a full revolution, and a
  night pass, from the data. A 1-hour `RANGE` window plus a running total feeds
  the `iss_distance` odometer on the page.
- **Satellites.** A 1-second `TUMBLE` join pairs each satellite position with
  the ISS fix from the same second and keeps anything inside 500 km. `LAG`
  over each satellite's own distance series (`PARTITION BY norad_id`) finds the
  second they were closest. Under 40 km, that second is a flight-log event.
- **The page.** FastAPI replays about one orbit of telemetry on startup, then
  follows live. A pipeline strip shows the rate on each topic and how many
  seconds the browser sits behind the station, usually about 3. On a narrow
  screen the page scrolls, and the flight log scrolls in its own box.

```
wheretheiss.at (1/s) ──► [iss_position]       ┐
                                              ├──► Flink SQL ──► SSE ──► globe
CelesTrak SGP4 (~6,000/s) ──► [sat_positions] ┘
```

## What it demonstrates

- **A pipeline you can watch.** The strip names `iss_position`,
  `sat_positions`, `iss_telemetry`, `iss_distance`, `iss_stats`,
  `iss_proximity`, and `iss_events`, each with a live rate or a count.
- **Now, and the next few hours.** "Nearest right now" comes from Flink: who
  is closest this second, often hundreds of kilometres away, and how many
  satellites sit inside 500 km. Under that, the server lists the next three
  passes under 40 km, predicted from orbital elements for the coming six hours
  and refreshed every 15 minutes. A pass that close lasts only seconds. The
  flight log is where the forecast meets the stream: Flink's event lands on
  the same second.
- **Numbers you can check against the published orbit.** The measured period
  should come out near 92.9 minutes. Ground-track speed is about 24,900 km/h,
  the distance between fixes on the rotating Earth. The API's orbital velocity
  is about 27,600 km/h, because the orbit runs roughly 420 km higher. Altitude
  rises and falls by about 12 km each revolution.
- **Declarative jobs.** The pipelines are 16 SQL files. Terraform submits each
  one as a Flink statement. The pool is capped at 20 CFUs. Set
  `satellites_enabled = false` for the ISS-only demo, or
  `conjunction_radius_km = 30` to drop below the big Starlink shell, under
  1,000 satellites.

## Technical details

- **Stack:** Python producers and a FastAPI dashboard on one `t4g.small`,
  Confluent Cloud (Basic Kafka, Schema Registry, Flink SQL), Terraform for
  both sides, CloudFront in front of the instance, credentials in Secrets
  Manager, shell access through SSM.
- **Schemas live in Flink.** `CREATE TABLE` creates each topic and registers
  its Avro schema. The producers fetch that schema from Schema Registry.
- **Event time.** Every table uses the fix timestamp, with a 2-second
  watermark, so windows and pattern matches follow the station's clock.
- **One partition per topic.** Ordering is trivial at these rates, and an idle
  partition cannot hold a watermark back. `sat_positions` keeps about an hour,
  on the order of 6,000 messages or 250 KB a second.
- **The globe draws in place.** Paths and labels reuse the same objects each
  frame. Satellite dots are two `THREE.Points` layers with fixed buffers.
  globe.gl treats a brand-new data object as a new mesh, and the shader
  compile that follows stalls the frame.
- **Public elements, a few kilometres.** That is enough to say a satellite
  passed 38 km away. Avoidance manoeuvres need tighter screening than this
  catalog provides.

## Challenges & lessons

1. **Flyovers have to be stateless.** After `UNNEST`, partitioning by city
   drops the event-time attribute, so a per-city window never gets a clock.
   Enrichment carries the previous two fixes on every telemetry row, and the
   flyover statement compares three haversine distances on that single row.
2. **A live globe wants the uncommitted read.** Flink commits its Kafka
   transactions about once a minute. The dashboard sets
   `isolation.level=read_uncommitted`, so telemetry shows up within about 3
   seconds. Statements that read Flink's own output still wait for the commit,
   so stats and flight-log events arrive in roughly one-minute batches.
3. **The join key is the second.** The two producers do not share a key. Both
   stamp a whole second, and the proximity statement joins 1-second tumbling
   windows on `window_start` and `window_end`.
4. **A replaced instance must not call CelesTrak.** The catalog service
   refuses a second download of the same data from one address, and the host
   keeps its Elastic IP. Each successful download is copied to S3. Boot
   restores that file and its original timestamp, and a file lock keeps the
   two Python processes from fetching at the same time.
5. **Replacing the app leaves a hole in the orbit.** A change under `app/`
   rebuilds the bundle and replaces the instance on the next `make infra`,
   which drops a few minutes of fixes. A replaced Flink statement also
   replays from the earliest offset and can re-emit events. The dashboard
   drops duplicates by event type, title, and time.
6. **The flight log has to survive a phone.** Below 1100px the globe,
   readouts, and log stack on a scrolling page. The event list keeps its own
   scroll box, about 65vh tall, so a flyover or a close approach stays
   readable while the rest of the page moves with the thumb.
