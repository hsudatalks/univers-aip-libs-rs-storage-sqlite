# SQLPOOL1 initial SQLite pool open

Candidate: `0.1.2-dev.20261013.1`, based on `928b53f`, on `develop`.
No publication or installed runtime mutation is part of this delivery.

## Startup contract

`open`, `open_with_limits` and `open_maintenance_pool` bootstrap a direct SQLx
connection outside the pool acquisition deadline. Connection-local foreign keys,
FULL synchronous mode and disabled auto-checkpointing are applied at connection
creation. The bootstrap inspects the journal mode and selects WAL once if needed.
Future pool and passive-checkpoint connections inherit persistent WAL instead of
repeating the mode transition. The initialized connection is transferred into a
private pool slot before the pool is returned; no temporary in-memory connection
can be observed by consumers. Pool growth and write/reopen tests cover this.

Startup busy handling uses at least five seconds and eight attempts per phase,
with exponential 25 ms backoff capped at one second. Only BUSY/LOCKED primary and
extended codes, plus interrupted/would-block I/O errors, retry. Missing storage,
permissions, corrupt files and persistent I/O errors return typed repository
errors immediately; exhausted contention names its phase and attempt count.
Owner-selected request acquisition and busy timeouts are restored after startup.
A filesystem call that never returns requires owner cancellation; no short
wall-clock deadline can distinguish that from slow healthy filesystem progress.

## Reproduction and owner checks

`scripts/sqlpool-pressure.sh` runs a bounded concurrent fsync writer on the same
filesystem and injects 6.25 seconds into each task-owned database's first rollback
journal fsync. The actual fsync still executes, preserving durability. The
pre-fix test binary from the fetched base fails at five seconds with
`RepositoryError::Timeout`; the same harness runs the candidate across default,
one-second explicit and maintenance limits, checking durable write/reopen each
time. Normal checks leave the pressure test ignored; explicit acceptance runs it.
The final candidate passed 30/30 with the concurrent writer and throttle enabled.
This proves the startup deadline failure mechanism under controlled latency;
it does not establish fsync as the exclusive cause of every original server case.

```bash
bash scripts/check.sh
bash scripts/build.sh
SQLPOOL1_ROOT="$HOME/.cache/univers/scratch/sqlpool1" \
  bash scripts/sqlpool-pressure.sh cargo test --locked --test initial_open_pressure \
  -- --ignored --nocapture
```

The normal owner check passed 89 tests (including doctest), with one explicit
pressure test ignored. Regression coverage includes pool expansion, shared
durable rows, per-connection pragmas, bounded request acquisition, transient WAL
contention, eight-attempt exhaustion, missing parent storage, corruption, and
read-only WAL failure. Final pressure and build receipts accompany the delivery.
The required `scripts/build.sh` release build gate also passed.

## Consumer acceptance and required admission

All consumer source/tests and original manifests/locks are untouched. Acceptance
uses task-owned manifest/lock projections and source links, admitting
`>=0.1.2-dev.20261013.1, <0.1.3` only in scratch and overriding the Univers package
to this owner source. Resolution changes only the SQLite package. A preliminary
Development smoke run used a scratch version alias; final acceptance uses the
actual new version. No second Git clone, owner gate bypass, or publication occurs.

| Consumer | Source revision | Same-pressure targeted result |
| --- | --- | --- |
| Development | `3da3fe91bb3f0c9ccdf86ca4960773023a898582` | Handoff test passed, 1/1 |
| Automation | `c0383beb55d453ee5f9dad84f00d06a71f94f262` | Entire row20 group passed, 4/4 |
| Data lifecycle | `0b3cb448e9d4536376ffd5fec027ffb3386746c9` | 1 passed, 6 failed, 1 process helper ignored |

Automation's row20 test file is byte-identical to the frozen failing revision
`c7540517b9a4f91277ea96df852ad4ac09ad6bd7`. Consumer fingerprints and exact
commands/results are retained with the delivery evidence.

All three need explicit admission of this prerelease followed by lock refresh:
Development's `routers/univers-studio-routers` currently uses a stable-only
`>=0.1.1, <0.2.0` range; Automation's Scripts/Automation services, Automation
routers and workflow-provider fixture pin `=0.1.1`; Data's sqlite-lifecycle,
record-kv and record-kv-legacy adapters also pin `=0.1.1`. A dependency bump alone
does not fix Data's separate startup implementation.

## Data owner blocker

`DataSqliteBackend::open_inner`, `sqlite-lifecycle/src/lib.rs:203-225`, owns its
SQLx pool directly: WAL connect options, `acquire_timeout(options.timeout)`, and
an initial `pool.acquire()`. It intentionally bypasses the shared opener to keep
an explicit joined close fence. Replacing this library therefore leaves Data's
initial deadline in place. All four named LQ7J lifecycle failures reproduce;
two additional physical-file lifecycle cases fail the same startup phase.

The Data owner must separate durable startup from request acquisition budgets,
using bounded transient retry and phase errors, while preserving canonical-path
reservation, physical identity binding, create-versus-existing policy, issued ID
codec, cancellation, and joined close/checkpoint fences. It must keep the existing
assertions and cannot substitute the shared opener's unjoined checkpoint worker.
That change is outside this library's authority and remains a release blocker.

Evidence is retained at `~/agent-reports/sqlpool1-evidence/`; the four-line report
is `~/agent-reports/sqlpool1.md`. Task scratch is removed after receipts are saved.
