# Generic SQLite storage

SQLite adapters over the published C0 Data storage Ports: typed JSON entity
repositories, graph repositories, query translation, transactional snapshot and
schema migrations, explicit JSON references, and SQLite error classification.
Consumers supply entity types, table names, schema definitions and scope policy.
No Framework, World or Task implementation is a dependency.

`open_with_limits(url, max_connections, timeout)` accepts explicit request pool
settings. `open(url)` uses 64 connections and five-second request acquisition and
busy timeouts; it does not read environment variables. These limits do not cancel
initial durable open on a slow disk. Startup opens a direct connection, configures
connection-local pragmas, checks/selects WAL once, and transfers that connection
into a ready pool. Subsequent request and checkpoint connections inherit the
database's WAL mode without repeating the mode transition. FULL synchronous
durability, foreign keys, disabled request auto-checkpoints, and the existing
passive checkpoint policy are preserved. `open_maintenance_pool` uses the same
startup path; maintenance callers own lifecycle coordination and database scope.

Startup uses at least a five-second SQLite busy timeout and up to eight attempts
per phase, with 25 ms exponential backoff capped at one second. Only BUSY/LOCKED
(including extended codes) and interrupted/would-block transport errors retry.
There is no wall-clock deadline around filesystem progress: an operating-system
call that does not return still requires owner cancellation. Permission, missing
directory, corruption and persistent I/O failures surface immediately as typed
`RepositoryError` variants. Every startup diagnostic names the phase and attempt.
After startup, caller-selected busy and pool acquisition timeouts still apply.
`open_in_memory()` provides an isolated single-connection test database.

Task Comment/Approval Notification writer fences are owner policy and were not
moved into this library. Consumers requiring those triggers must retain them in
the Task owner, using the public SQLite primitives. Environment loading such as
`SQLITE_MAX_CONNECTIONS` also remains with the consuming owner.

The initial generic implementation and its applicable tests come from
`hvac-workbench` commit `04b106a8467975276bfe70577b3d1e824521492e`, `apps/plugins/univers-storage-sqlite`.
Task-specific trigger code/tests were excluded. Public Operation Task value
contracts appear only as test fixtures for generic index behavior.

Version 0.1.1 keeps `encoded_metadata.*` extraction for legacy JSON strings,
but an absent field now stays SQL `NULL`. The SQLite adapter no longer maps a
missing or blank `encoded_metadata.organization_id` to the `default` tenant or
tries `organizationId` as an implicit alias. Owners that need legacy records
to appear under an organization must explicitly migrate them or select an
owner-controlled compatibility query. In particular, a paged list/count over
`encoded_metadata.organization_id` can return fewer rows than 0.1.0 until the
owner resolves old records; this is deliberate and does not delete those rows.

`ensure_json_index(pool, table, spec)` validates the C0 table identifier and
index specification, then creates the same SQLite `json_extract(data, ... )`
ascending/descending index as the old storage adapter. The caller selects the
pool and table and owns index lifecycle; no owner DDL or migration policy is
loaded from the environment.

Version 0.1.2-dev.20261012.1 preserves issued Data SQLite IDs by default in
all four repository constructors (`SqliteIdCodec::DataLegacy`): lookups and
mutations strip everything through the last colon, even another table prefix;
stored IDs containing a colon materialize verbatim, while bare stored IDs gain
the repository table prefix. This includes legacy namespace aliasing; no stored
bytes are rewritten and no owner change is required for Data's lifecycle adapter.

Consumers requiring the namespaced key behavior introduced in
0.1.2-dev.20261002.1 must select
`.with_id_codec(SqliteIdCodec::TableQualified)` after construction. That codec
strips only the current table prefix once and always qualifies materialized IDs;
use the same codec on reopen. Both codecs retain the graph-compatible adapter.

Add `univers-aip-lib-storage-sqlite = {version=">=0.1.2-dev.20261013.1, <0.1.3", registry="univers"}`
after this candidate's separate publication batch. Prereleases require explicit
consumer admission; existing stable-only ranges do not select this candidate.
The release lock validates C0 Data and Operation rc.2. Run
`bash scripts/check.sh`, `bash scripts/build.sh`, and from a clean committed
candidate `bash scripts/publish.sh`. Credentials remain outside Git.

Linux reliability reproduction (a concurrent fsync-heavy writer plus a controlled
6.25-second journal-fsync throttle; real fsync still runs, and the helper only
affects task-owned files under `SQLPOOL1_ROOT`):

```bash
SQLPOOL1_ROOT="$HOME/.cache/univers/scratch/sqlpool1" \
  bash scripts/sqlpool-pressure.sh cargo test --locked --test initial_open_pressure \
  -- --ignored --nocapture
```

The test performs 30 opens across default, one-second explicit, and maintenance
limits, then verifies write/reopen durability. Use `SQLPOOL1_RUNS=1` for the
pre-fix reproduction. Retain evidence and remove task scratch when done.
