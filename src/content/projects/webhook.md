---
title: 'The Friendly Webhook — Webhooks Explained in ~30 Lines'
description: 'A tiny, fully-working webhook receiver in Python, paired with the clearest explanation of webhooks you will find — push vs. poll, and how a webhook bridges to Kafka.'
pubDate: 2026-07-23
tags: ['Python', 'Flask', 'Webhooks', 'HTTP', 'Kafka']
status: 'completed'
repo: 'https://github.com/MosheBlumbergX/webhook'
image: '/webhook.svg'
---

![Three event cards — payment, signup and push — each answered with 200 OK, over the flow event → POST /webhook → 200 → produce → kafka topic](/webhook.svg)

## Overview

You order a pizza. How do you find out when it arrives?

You *could* walk to the window every thirty seconds to check — see nothing, sit
down, get up, check again — for forty minutes. That's **polling**, and it's what
a lot of beginner code does: *"any news yet? any news yet?"*, over and over,
wasting effort on a question whose answer is almost always "not yet."

Or you could hand over your phone number and get on with your life, until it
buzzes: *"your pizza is on its way."* The news came **to you**, exactly when it
mattered.

That second version is a **webhook** — a URL you hand to another service so it can
call *you* the instant something happens. This project is that phone number, in
about thirty lines of Python, wrapped in the clearest explanation of webhooks I
could write.

## How it works

The entire receiver is a small Flask app. Only three things really matter:

- **The address** — `@app.route("/webhook", methods=["POST"])` is your phone
  number. It answers only `POST`, because that's how senders deliver data.
- **Reading the message** — `request.get_json()` pulls out whatever the sender
  left for you.
- **The acknowledgement** — `return {"status": "ok"}, 200` says *"got it, thanks!"*
  so the sender doesn't assume failure and keep retrying.

```
event ──POST──► /webhook ──► return 200 ──► (optionally) produce ──► [kafka topic]
```

Everything else in the webhook world — Stripe, GitHub, Slack — is just this idea
with extra polish.

## The one idea to remember: push vs. pull

Webhooks feel magical until you see the single principle underneath — **who makes
the call.** With *polling* you ask, constantly, and waste effort. With a *webhook*
you wait, and the other service initiates the moment there's news. Instant, and
effortless. Keep that contrast in mind and every webhook system you meet makes
sense.

## Bonus: can a webhook work as a Kafka consumer?

A question that comes up a lot — and the answer reveals a nice subtlety. A webhook
and a Kafka consumer are **opposite delivery models**:

- A **webhook is push** — someone calls your HTTP endpoint; you don't control the
  timing or rate.
- A **Kafka consumer is pull** — *you* run a loop that polls the broker at your own
  pace, and can rewind and replay old messages.

So a webhook can't literally *be* a Kafka consumer. Instead you **bridge** them:
the webhook endpoint receives the event, immediately **produces** it to a Kafka
topic, and returns `200`; a separate consumer processes it afterwards. That buys
you buffering against spikes, durability if a downstream service is down, fan-out
to many consumers, and replay from the log.

## Technical details

- **Server:** Python + Flask, reading configuration from a `.env` file via
  `python-dotenv`.
- **Developer experience:** heavily commented source, a five-command quick start,
  and `print(..., flush=True)` so received events show up immediately.
- **Honest troubleshooting:** documents the classic macOS gotcha where port `5000`
  is silently held by the AirPlay Receiver — which is exactly why the app defaults
  to port `5001`.

## Challenges & lessons

1. **"Nothing prints" is usually the port, not the code.** On macOS, AirPlay
   Receiver quietly owns port 5000, so Flask fails to bind and `curl` ends up
   talking to AirPlay. Defaulting to 5001 removes the most common first-run
   confusion.
2. **Buffered stdout hides success.** Python buffers `print()` when output isn't a
   terminal; `flush=True` guarantees each event is visible the moment it arrives.
3. **The clearest explanation is a story.** Framing polling vs. webhooks as
   "checking the window" vs. "getting a text" does more to teach the concept than
   any API reference.
