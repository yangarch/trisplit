# Training Companion / trisplit

This is the maintained code source for one integrated project: collection,
lakehouse, reports, three-sport AI coaching, history and notifications.
`apps/coach` replaces the separately maintained fingu code after operational
cutover. Read `docs/consolidation.md` and `apps/coach/README.md` for migration,
deployment flags, legacy status and rollback. Do not infer cutover completion
from code being present; check the private workspace's current STATUS.md.

The sibling `training-companion` directory is the active **private data and
conversation workspace**, not a retired code repository. Read its STATUS.md and
CLAUDE.md for user decisions and operational context. Do not put new personal
health records, GPS, images, DBs, tokens, generated reports or real-data test
fixtures in this public code repository. Keep fixtures synthetic and verify
new ignore rules using `git check-ignore`.

The pipeline is the only collector. The coach consumes validated canonical
bundles after `dbt_test`; it must not add a second intervals.icu poller. Retain
the existing numeric activity ID contract, deduplication and swim rest handling.
Preserve missing data versus measured zero. Do not invent symptoms, technique
observations or training intent from telemetry. FTP is a user-maintained anchor.

Use `python3 -m unittest discover -s tests -v` for pipeline/pacing regressions
and `npm test --prefix apps/coach` for coaching, migration, auth and outbox tests.
Tests must not call paid models or send external notifications. Private staging
may export real bundles and migrate DB copies with processing disabled.
Keep old repositories and DBs intact until migration and single-sender cutover
are verified; do not archive an active sender or merge personal data into Git.
