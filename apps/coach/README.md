# Training Companion coach

The coaching application in the unified trisplit repository. It replaces fingu's
independent collection and swim-only analysis after the migration is activated.
It accepts only canonical, quality-checked cycling/running/swimming bundles from
the lakehouse. It has **no intervals.icu credentials or polling**.

## Develop and verify

```sh
npm ci
npm test
```

Tests use synthetic records, a temporary SQLite DB, mocked model/notification
calls and localhost HTTP. They never call an AI provider or send Slack messages.
Node 20 is the container runtime. Dependencies start from fingu's lock, with
unused axios/ts-node-dev removed and compatible security fixes applied. `qs`
is overridden to the patched 6.16.1+ range because Express's old minor constraint
otherwise retains an affected version.

To validate real, private pipeline output without a worker or model:

```sh
node dist/preview.js /private/path/to/coach-outbox
```

This validates every bundle and tests repeated queue insertion in an in-memory
DB. It prints only aggregate counts and never prints the workout payloads.

`services/swimMetrics.ts` retains fingu's distance-stall rest reconstruction.
The adapter rejects non-monotonic/incomplete streams and gaps over five seconds
instead of filling missing sensor data. Inferred rests are labeled estimates.
The prompt does not invent coaching instructions, technique observations,
symptoms or session intent from lap patterns.

## Configuration and deployment

Copy `.env.example` to `.env`. Set an explicit owner, cutover date, independent
ingest/admin/session secrets (at least 32 characters), and a private DB directory.
Keep `COACH_ENABLED=false` and `COACH_SEND_NOTIFICATIONS=false` while reviewing.
Configure `COACH_DATA_DIR` for Compose; `COACH_PORT` defaults to a separate 3004
so the legacy service can remain on 3003 during verification. `COACH_BIND_IP`
defaults to loopback; expose the ingest endpoint only through your private
network or an authenticated TLS proxy.

With these flags off, startup, migration, API ingestion and dashboard viewing
perform **no model calls or message sending**. When analysis is enabled but
notifications are off, deliveries are stored as `suppressed` and are never
automatically sent later. Select a fresh cutover date for the live DB.

### Preserve existing history

Never mount the live legacy database into the new application. Preview first:

```sh
node dist/migrate.js --source /private/legacy/swim-analyzer.db
node dist/migrate.js --source /private/legacy/swim-analyzer.db \
  --target /private/coach/coach.db --owner OWNER_ID --apply
```

The source is opened read-only. SQLite's online backup includes committed WAL
pages, so a running DB is not copied as an isolated `.db` file. The target must
not exist. All athletes and old tables are preserved in the private backup;
`swim_analyses` is copied into `activity_analyses` with sport `swimming` and every
legacy processed activity is excluded from new analysis/notification. This also
preserves old processed entries which lack an analysis row. All analysis text
and owner IDs are compared during migration. The dashboard shows only the
explicit configured owner and ignores old unsigned `athlete_id` cookies.

The source and copied DBs contain private records and may contain old tokens.
Keep them outside the code repository. Do not migrate them through Git.

### Ingest and processing

- `POST /api/workouts`: bearer `COACH_INGEST_TOKEN`, schema v1 bundle; durable
  enqueue by activity ID. An upload acknowledgement does not mean AI analysis
  or Slack delivery has completed.
- `GET /api/status`: same bearer token; aggregate job, delivery and alert states.
- `GET /health`: no personal data. This reports process availability, not backlog
  completion; inspect `/api/status` for errors/uncertain delivery.
- `GET /login`, `POST /login`: admin-token login, signed HttpOnly session cookie.
  `COOKIE_SECURE=true` requires HTTPS at the browser-facing proxy.
- `GET /dashboard`: authenticated three-sport history, plus an optional
  `COACH_REPORT_URL` link to the existing protected lakehouse report.
- `GET /swim-notes`: authenticated swim lookup and import of diary links.
  Clear matches are saved on import, ambiguous matches need a selection, and
  a previously linked URL opens the existing record. The public swimmingdiary
  board supplies post metadata. Other HTTPS links can use an explicit date.
- `GET /swim-notes/ACTIVITY_ID`: attach, edit, or remove a note in the owner's
  private DB. Set `COACH_PUBLIC_URL`
  to the HTTPS coach origin to include this route in future swim alerts.
- `POST /api/swim-catalog`: bearer-authenticated canonical swim lookup data
  containing only IDs, local start times, distances and moving times. The
  publisher sends changes after the existing quality gate. Deploy the receiver
  before the publisher. Note links and text stay out of model and alert inputs.

The worker claims one job at a time. Analysis text, processed history and the
notification outbox are committed in one SQLite transaction. Model failures
remain unprocessed and retry with backoff, at most five attempts. One failure
alert per activity is queued when notifications are enabled. Prompts and API
errors containing request credentials are never logged.

There is no exactly-once delivery guarantee from incoming Slack webhooks. A
timeout/5xx or restart during sending is `unknown` and **does not auto-retry**.
Non-success 4xx, including rate limiting, is `failed` and needs inspection.
After checking the channel, an operator can explicitly retry:

- `POST /api/retry/analysis/ACTIVITY_ID`: retry an exhausted analysis job.
- `POST /api/retry/delivery/ACTIVITY_ID` with JSON
  `{"confirm_checked":true}`: retry a failed/unknown notification.

Both require the ingest bearer token. Successful, suppressed and historical
notifications cannot be replayed by these endpoints. A process restart recovers
unfinished model work; it never blindly replays an in-flight notification.

See [the consolidation runbook](../../docs/consolidation.md) for quality gates,
pipeline wiring, legacy shutdown order and rollback.
