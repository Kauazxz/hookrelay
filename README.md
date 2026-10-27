# hookrelay

[![CI](https://github.com/Kauazxz/hookrelay/actions/workflows/ci.yml/badge.svg)](https://github.com/Kauazxz/hookrelay/actions/workflows/ci.yml)

**A tiny webhook gateway that verifies, de-duplicates and reliably forwards webhooks.**

hookrelay sits between a webhook producer (a payment provider, a Git host, a SaaS) and your
application. It checks the HMAC signature, drops re-deliveries it has already seen, stores
each event durably, and forwards it to your service with retries, exponential backoff and a
dead-letter state you can inspect and replay.

It is a single static Go binary built only on the standard library: no third-party modules.

## Why

I run the online store for a university athletic association
([Tubarões UVV](https://tubaroesuvv.com.br)). Payments go through Mercado Pago, which notifies
the store by webhook, and like every payment provider it **delivers at least once**. If the
store does not answer fast enough, or the network drops the response, the same notification
arrives again, sometimes several times. A naive handler marks the order paid and **decrements
stock twice** for one payment.

In that project I solved it inside the app: verify the HMAC signature, take an idempotency key
from the payload, record it once, and treat every repeat as a no-op. hookrelay pulls that
pattern out into a reusable service, adding what an in-app handler usually lacks: durable
storage of every event, retries towards your own service, and a way to see and replay what
failed.

## How it works

```mermaid
sequenceDiagram
    autonumber
    participant P as Provider
    participant R as hookrelay
    participant S as Store (JSONL)
    participant D as Dispatcher
    participant T as Your service

    P->>R: POST /hooks/payments (body, X-Signature, X-Timestamp)
    R->>R: verify HMAC-SHA256 (constant time) and timestamp tolerance
    R->>S: Create(source, idempotency key)
    alt first time this key is seen
        S-->>R: created (fsync'd)
        R-->>P: 202 {"id": "evt_…", "duplicate": false}
        R--)D: notify
    else re-delivery
        S-->>R: existing event
        R-->>P: 200 {"id": "evt_…", "duplicate": true}
    end
    loop until 2xx or max_attempts
        D->>S: ClaimDue (lease)
        D->>T: POST target_url (X-Hookrelay-Event-Id, X-Hookrelay-Attempt)
        alt 2xx
            D->>S: status = delivered
        else error or non-2xx
            D->>S: attempts++, next_attempt_at = now + backoff with jitter
        end
    end
    Note over D,S: after max_attempts the event is "dead";<br/>POST /events/{id}/replay requeues it
```

## Quickstart

Requires Go 1.24+ (or Docker).

```bash
git clone https://github.com/Kauazxz/hookrelay && cd hookrelay

# Something to forward to. Any HTTP server that answers 2xx works.
python3 -m http.server 9000 &   # answers POST with 501, so you can watch retries; use your own app for 2xx

export HOOKRELAY_SECRET_PAYMENTS=whsec_demo   # HMAC secret for the "payments" source
export HOOKRELAY_ADMIN_TOKEN=admin            # bearer token for the admin endpoints
go run ./cmd/hookrelay -config config.example.json
```

`config.example.json` defines one source, `payments`, that expects
`X-Signature: sha256=<hex>` over `"<X-Timestamp>.<body>"`, rejects timestamps older than five
minutes, and forwards to `http://localhost:9000/webhooks/payments`.

### Send a signed webhook

```bash
BODY='{"type":"payment.succeeded","data":{"id":"pay_123","amount":4990}}'
TS=$(date +%s)
SIG=$(printf '%s.%s' "$TS" "$BODY" | openssl dgst -sha256 -hmac "$HOOKRELAY_SECRET_PAYMENTS" | sed 's/^.* //')

curl -i -X POST localhost:8080/hooks/payments \
  -H 'Content-Type: application/json' \
  -H "X-Timestamp: $TS" \
  -H "X-Signature: sha256=$SIG" \
  -d "$BODY"
# HTTP/1.1 202 Accepted
# {"id":"evt_81f8a1f199e9b9929bb3d0ee","duplicate":false}
```

Send exactly the same request again (the provider re-delivering):

```bash
# HTTP/1.1 200 OK
# {"id":"evt_81f8a1f199e9b9929bb3d0ee","duplicate":true}
```

The key here is `data.id` (`pay_123`), so even a re-delivery with a different body or timestamp
is recognized. A wrong signature gets `401 {"error":"signature mismatch"}`, and a correctly
signed request with a timestamp older than the tolerance gets
`401 {"error":"timestamp outside tolerance"}`.

### Inspect and replay

```bash
AUTH='Authorization: Bearer admin'

curl -s -H "$AUTH" 'localhost:8080/events?source=payments&status=dead'   # list (newest first, ?limit=1..500)
curl -s -H "$AUTH" localhost:8080/events/evt_81f8a1f199e9b9929bb3d0ee    # one event, with its body
curl -s -X POST -H "$AUTH" localhost:8080/events/evt_81f8a1f199e9b9929bb3d0ee/replay
curl -s localhost:8080/healthz                                            # {"status":"ok"}
```

Replay only works on `dead` events (`409` otherwise). It resets the attempt counter and sends the
event straight back to the dispatcher.

### Docker

```bash
docker build -t hookrelay .
docker run --rm -p 8080:8080 -v hookrelay-data:/data \
  -e HOOKRELAY_SECRET_PAYMENTS=whsec_demo -e HOOKRELAY_ADMIN_TOKEN=admin \
  -e HOOKRELAY_TARGET_PAYMENTS=http://host.docker.internal:9000/webhooks/payments \
  hookrelay
```

The image is distroless (`static-debian12:nonroot`), runs as UID 65532 and keeps the event log
in the `/data` volume.

## API

| Method and path | Auth | Description |
| :-- | :-- | :-- |
| `POST /hooks/{source}` | HMAC signature | Ingest a webhook. `202` new, `200` duplicate, `401` bad signature or stale timestamp, `404` unknown source, `413` body too large. |
| `GET /events?source=&status=&limit=` | Bearer | List events, newest first. `status` is `pending`, `delivered` or `dead`. Default limit 50. |
| `GET /events/{id}` | Bearer | One event, including its body (as JSON when the body is JSON). |
| `POST /events/{id}/replay` | Bearer | Requeue a dead-lettered event. `409` if it is not dead. |
| `GET /healthz` | none | Liveness. |

Forwarded requests carry the original body and `Content-Type`, plus `X-Hookrelay-Event-Id`,
`X-Hookrelay-Source`, `X-Hookrelay-Idempotency-Key` and `X-Hookrelay-Attempt`. The producer's
signature header is **not** forwarded: it was verified at the edge, and the secret belongs to
the relay, not to your service.

## Configuration

Settings come from a JSON file (`-config path`, or `HOOKRELAY_CONFIG`, default `config.json`).
Environment variables override the file. Unknown keys are rejected, so a typo fails at startup
instead of silently falling back to a default. Secrets should only ever be set through env.

| JSON key | Env override | Default | Meaning |
| :-- | :-- | :-- | :-- |
| `addr` | `HOOKRELAY_ADDR` | `:8080` | Listen address. |
| `log_level` | `HOOKRELAY_LOG_LEVEL` | `info` | `debug`, `info`, `warn`, `error`. Logs are JSON (`log/slog`). `debug` adds one line per HTTP request. |
| `max_body_bytes` | | `1048576` | Larger bodies get `413`. |
| — | `HOOKRELAY_ADMIN_TOKEN` | *(empty)* | Bearer token for `/events*`. Env only. If unset, the admin API rejects every request. |
| `store.type` | `HOOKRELAY_STORE` | `memory` | `memory` or `file`. |
| `store.path` | `HOOKRELAY_STORE_PATH` | | JSONL log path for the file store. The directory is created if missing. |
| `dispatcher.workers` | `HOOKRELAY_WORKERS` | `4` | Concurrent deliveries. |
| `dispatcher.max_attempts` | `HOOKRELAY_MAX_ATTEMPTS` | `8` | Attempts before an event is dead-lettered. |
| `dispatcher.backoff_base` | | `1s` | Delay after the first failure; doubles on each attempt. |
| `dispatcher.backoff_max` | | `5m` | Upper bound on the delay. |
| `dispatcher.request_timeout` | | `10s` | Timeout of one forward request. |
| `dispatcher.poll_interval` | | `1s` | How often due retries are scanned (new events go out immediately). |
| `sources.<name>.secret` | `HOOKRELAY_SECRET_<NAME>` | | HMAC-SHA256 key. Required. |
| `sources.<name>.signature_header` | | | Header carrying the hex signature, e.g. `X-Signature`. Required. |
| `sources.<name>.signature_prefix` | | | Prefix stripped before decoding, e.g. `sha256=`. |
| `sources.<name>.timestamp_header` | | | If set, the signed payload is `"<timestamp>.<body>"` (unix seconds). |
| `sources.<name>.tolerance` | | `0` (off) | Maximum clock skew or age of the timestamp, e.g. `5m`. Needs `timestamp_header`. |
| `sources.<name>.idempotency_header` | | | Header with the idempotency key, e.g. `Idempotency-Key`. |
| `sources.<name>.idempotency_field` | | | Dot path into a JSON body, e.g. `data.id`. String or number. |
| `sources.<name>.target_url` | `HOOKRELAY_TARGET_<NAME>` | | Where events are forwarded. Absolute `http(s)` URL. Required. |

`<NAME>` is the source name in upper case with `-` replaced by `_` (`my-shop` becomes
`HOOKRELAY_SECRET_MY_SHOP`). Source names match `^[a-z0-9][a-z0-9_-]{0,63}$`.

**Idempotency key resolution:** header, if configured and present; else the JSON field, if
configured and present; else `sha256:<hex of the body>`. Keys are scoped per source.

## Design decisions

**At-least-once delivery plus idempotency, not "exactly once".** Exactly-once delivery over a
network is not something a relay can promise: the target can process a request and the response
can still be lost. So hookrelay guarantees two narrower things that compose into what you
actually want:

1. *Ingestion is exactly-once per idempotency key.* The check-and-insert happens under one lock,
   and the event is fsync'd before the producer gets `202`. Twenty concurrent copies of the same
   delivery produce one event (there is a test for exactly that).
2. *Forwarding is at-least-once.* A claimed event is leased (hidden from other claims) for three
   request timeouts. If the process dies mid-delivery, the lease expires and the event is sent
   again. Your service should therefore de-duplicate on `X-Hookrelay-Event-Id`, which stays the
   same across every attempt and replay. That is one indexed column, much simpler than
   understanding each provider's retry semantics.

**Backoff with "equal jitter".** The delay after attempt *n* is `base * 2^(n-1)`, capped at
`backoff_max`, and then drawn uniformly from `[d/2, d)`. The jitter matters when many events
fail at once (your service was down): without it they all retry in lockstep and hit the service
as a wave the moment it comes back. The lower bound of `d/2` keeps the delays from collapsing
to zero. Every non-2xx response and every network error is retried.

**Why an append-only JSONL file for the durable store.** It is the smallest thing that is
actually durable and that a reviewer can audit in a few minutes (about a hundred lines). Each
change appends the full event snapshot as one line and calls `fsync` before acknowledging. On
startup the log is replayed, the last line per ID wins, and the in-memory index (by ID and by
source plus key) is rebuilt. A torn last line, from a crash in the middle of a write, is
detected (no trailing newline) and truncated; any other malformed line stops startup, because
silent corruption is worse than a loud failure. If a write or fsync fails, the file is cut back
to its last good length so disk and memory never disagree. The trade-offs are explicit: the
whole index lives in memory, the log is never compacted, and one process owns the file.

**Storage behind an interface.** `store.Store` has six methods (`Create`, `Get`, `List`,
`Update`, `ClaimDue`, `Close`). `Update` takes a function and runs it atomically, so a check
like "only replay if dead" cannot race with a concurrent change. `ClaimDue` with a lease maps
directly onto `SELECT … FOR UPDATE SKIP LOCKED` in Postgres, which is the next backend.

**Graceful shutdown.** On `SIGTERM`/`SIGINT` the HTTP server stops accepting webhooks and
finishes requests in flight; only then is the dispatcher cancelled. It stops claiming, returns
claimed-but-unstarted events to the queue, and waits for in-flight deliveries to finish and
record their outcome. Outbound requests deliberately do not inherit the shutdown context:
aborting a POST halfway would leave its outcome unknown.

**Security details.** Signatures are compared with `hmac.Equal` (constant time). When a
timestamp header is used it is part of the signed payload, so an attacker cannot pair an old
body with a fresh timestamp. The admin token is compared as a SHA-256 digest with
`subtle.ConstantTimeCompare`, which hides its length as well as its content. Request bodies are
capped with `http.MaxBytesReader`, and the server sets read, write and header timeouts.

## Testing

```bash
make test      # go test -race -count=1 ./...
make vet       # gofmt check + go vet
```

The suite runs in about two seconds and covers:

- signatures: valid, wrong secret, tampered body, missing header or prefix, non-hex, valid with
  timestamp, expired, timestamp swapped after signing, missing timestamp;
- idempotency keys: header first, JSON field, large numeric IDs kept exact, body-hash fallback;
- both stores, through the same tests: duplicate suppression, 50 concurrent creates of one key
  giving one event, lease-based claiming, atomic update and filtering;
- the file store surviving a restart, rebuilding the dedup index, truncating a torn final line,
  and refusing a corrupt log;
- the dispatcher, against `httptest` servers: retry with growing backoff until success,
  dead-letter after exactly `max_attempts` with no further calls, and shutdown waiting for an
  in-flight delivery;
- end to end over HTTP: bad requests are rejected and not stored; a re-delivery and 20
  concurrent copies are acknowledged but forwarded once; the admin API requires the bearer
  token; a dead event can be listed, replayed and delivered, and a delivered one cannot be
  replayed;
- config loading: env overrides, missing secrets, bad target URLs, unknown keys.

CI (`.github/workflows/ci.yml`) runs the gofmt check, `go vet`, the race-enabled tests, the
build and a Docker build.

## Limitations

Being explicit about what is **not** there yet:

- Single instance. The file store assumes one process owns the log; running two replicas on the
  same file would corrupt it. Horizontal scaling needs the Postgres backend.
- The JSONL log grows forever (every attempt appends a snapshot) and is never compacted, and the
  in-memory index holds every event, bodies included.
- Signatures are HMAC-SHA256 in hex, in one header, with an optional separate timestamp header.
  Provider formats that pack several values into one header (Stripe's `t=…,v1=…`, Mercado Pago's
  `ts=…,v1=…` over a manifest string) or use base64 are not supported out of the box yet.
- Forwarded requests are not signed by hookrelay; the target has to trust the network path.
- No metrics endpoint and no rate limiting.

## Roadmap

- [ ] Postgres store (`ClaimDue` with `FOR UPDATE SKIP LOCKED`, unique index on source and key).
- [ ] Log compaction for the file store (rewrite to a temp file, fsync, rename).
- [ ] Provider presets for Stripe, GitHub and Mercado Pago signature schemes; base64 signatures.
- [ ] Sign forwarded requests with a per-target secret.
- [ ] Treat permanent failures (most 4xx) as dead immediately instead of retrying.
- [ ] Prometheus metrics (accepted, duplicates, delivery latency, dead letters).
- [ ] Retention: delete delivered events after N days.

## Project layout

```
cmd/hookrelay/        main: config, wiring, signal handling, graceful shutdown
internal/config/      JSON + env loading and validation
internal/webhook/     HMAC verification and idempotency-key extraction (pure functions)
internal/store/       Store interface, in-memory store, append-only JSONL file store
internal/dispatch/    workers, retries, backoff with jitter, dead letters
internal/server/      HTTP handlers (Go 1.22+ method and path patterns), admin auth
```

## License

[MIT](LICENSE) © 2026 Kauã Araujo de Souza
