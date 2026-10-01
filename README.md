# Generic SQLite storage

SQLite adapters over the published C0 Data storage Ports: typed JSON entity
repositories, graph repositories, query translation, transactional snapshot and
schema migrations, explicit JSON references, and SQLite error classification.
Consumers supply entity types, table names, schema definitions and scope policy.
No Framework, World or Task implementation is a dependency.

`open_with_limits(url, max_connections, timeout)` accepts explicit connection
settings. `open(url)` uses fixed defaults of 64 connections and five seconds;
it does not read environment variables. `open_in_memory()` provides an isolated
single-connection test database. The existing WAL/passive checkpoint behavior is
retained; maintenance callers own lifecycle coordination and database scope.

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

Add `univers-aip-lib-storage-sqlite = {version="=0.1.2-dev.20261002.1", registry="univers"}`
to consume the namespace fix from the development Registry.
The release lock validates C0 Data and Operation rc.2. Run
`bash scripts/check.sh`, `bash scripts/build.sh`, and from a clean committed
candidate `bash scripts/publish.sh`. Credentials remain outside Git.

Record IDs remove only the current repository's `<table>:` prefix. A key such
as `planning:proposal_lifecycle` remains distinct from
`inspection:proposal_lifecycle` and `proposal_lifecycle`, including CAS updates,
bulk reads and deletes. Materialization restores the table prefix while keeping
the complete key. Older versions truncated at the last colon; already truncated
rows cannot be attributed to a namespace safely and are not automatically
aliased or migrated by this fix. The fix first landed in source commit
`1fb155a988cf8d5f29e88bd46a2626cd13fce509`; the published 0.1.1 predates it.

The physical `id` column always contains a logical key, and materialization
always prepends the known table once. For example, the qualified ID
`workflows:workflows:planning:proposal_lifecycle` stores the key
`workflows:planning:proposal_lifecycle` and returns the complete qualified ID.
An input starting with `workflows:` is interpreted as qualified; callers must
use the fully qualified form for keys that themselves start with `workflows:`.
A different prefix is part of the key, not another table's qualification.
All CRUD, CAS, bulk reads/deletes and query materialization use this contract.

No namespace lookup falls back to a shortened suffix. A historical
`proposal_lifecycle` row stays independently addressable, even when new
`planning:proposal_lifecycle` and `namespace_probe:proposal_lifecycle` rows exist.
Duplicate creation rejects the occupied key without replacing its data, including
soft-deleted rows. This release does not recover previously collapsed identities:
the consumer must supply an explicit, trusted mapping and its authorized data,
reject destination conflicts, and retain the source row and persistent recovery
history. The library cannot infer lost namespace ownership from a suffix.
