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

Add `univers-aip-lib-storage-sqlite = {version="=0.1.0", registry="univers"}`.
The release lock validates C0 Data and Operation rc.2. Run
`bash scripts/check.sh`, `bash scripts/build.sh`, and from a clean committed
candidate `bash scripts/publish.sh`. Credentials remain outside Git.
