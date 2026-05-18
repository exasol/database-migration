# Plan: Per-source parallel IMPORT optimization

## Status
Drafted 2026-05-17. Sibling speq landed as `parallel-row-threshold-dispatcher-gate/` (commit 928d30c on `feat/parallel-row-threshold`). Plan refactored 2026-05-17 (post-gate) to (1) introduce a cross-cutting split-strategy hierarchy that every adapter follows, (2) add **date-bucket** as a first-class strategy ahead of generic hash-modulo, (3) cap `PARALLEL_STATEMENTS=AUTO` at 12 to spare source systems.

## Motivation
Today only the Oracle adapter emits multi-statement parallel IMPORT. Its partition strategy is bin-pack on `all_tab_partitions` when partitions exist, else modulo split on `ora_hash(rowid)`. This works but:
- It is Oracle-only. Other adapters (`PostgreSQL`, `MySQL`, `MariaDB`, `SQL Server`, `Snowflake`, `BigQuery`, `Redshift`, `Vertica`, `Teradata`, `DB2`, `Trino`, `Dremio`, `ClickHouse`, `Databricks`, `DuckDB`, `StarRocks`, `Vectorwise`, `SAP HANA`) emit single-statement IMPORTs regardless of table size — there is no parallel option to offer the user, even when the source could clearly support it.
- Even within Oracle, `ora_hash(rowid)` ignores actual data layout. On a heavily-partitioned table where bin-pack works, fine; on a non-partitioned monster table on a NUMERIC primary key, `ora_hash(rowid)` may produce N statements that all hit the same physical blocks, giving worse parallelism than `MOD(pk, N)` would.

This speq covers both axes — improving Oracle's existing strategy AND bringing parallel emission to adapters that lack it, each using the source's native partitioning / hashing primitives.

## Dependencies
- **Hard dependency satisfied**: `parallel-row-threshold-dispatcher-gate/` shipped 2026-05-17 (commit 928d30c). Gate operates at dispatcher layer, so this speq's per-adapter parallel emission inherits the threshold collapse for free.
- Soft dependency on the wrapper audit table from PR #42 (`harden-master-migration-entrypoint`) — the new partition strategy column (see Audit changes below) should reuse that table's schema.

## Split-strategy hierarchy (cross-cutting)

Every adapter walks this ordered hierarchy per table; stops at first hit:

| # | Strategy | Selector key | Why this position |
|---|----------|--------------|-------------------|
| 1 | `PARTITION` — per native partition / sub-partition | source partition metadata view | engine-validated even distribution; pushdown-perfect |
| 2 | `PK_RANGE` / `PK_MOD` — numeric singleton PK | declared primary key, numeric | sequential IDs → uniform split; cheapest WHERE |
| 3 | `UNIQUE_NUM` — numeric singleton unique index | unique constraint | semantically equivalent to PK |
| 4 | `DATE_BUCKET` — date / timestamp column **(NEW)** | first DATE/TIMESTAMP col matching `*(_)?(date\|dt\|time\|day\|created\|loaded\|event\|posted)$`, else first DATE/TIMESTAMP by ordinal | pushdown-friendly on indexed cols; readable in audit; cheaper than hash; resilient where PK is composite / UUID |
| 5 | `HASH_NUM` — first numeric not-null col, hash modulo | column metadata | catches integer surrogate cols not flagged as PK |
| 6 | `ROWID` — engine-specific physical row identifier | dialect (Oracle `ROWID` / PG `ctid` / MSSQL `%%physloc%%` / DB2 `RID_BIT`) | always works on heap; zero metadata needed |
| 7 | `SINGLE` — emit one STATEMENT + `INFO` row "no parallel split found" | — | safe fallback; matches today's behavior |

`PARALLEL_SPLIT` OPTIONS knob (per migration; not per table):
- `AUTO` (default) — walk hierarchy
- `PARTITION` — force step 1; fail-soft to SINGLE if no partition
- `PK` — force step 2/3; fail-soft to SINGLE
- `DATE` — force step 4; auto-pick column
- `DATE:<col>` — force step 4 with named column
- `DATE:<col>:<grain>` — force step 4 with `MONTH` / `QUARTER` / `YEAR_MONTH` granularity
- `HASH:<col>` — force step 5 with named column
- `ROWID` — force step 6
- `OFF` — force SINGLE (equivalent to `PARALLEL_STATEMENTS=1`)

### Date-bucket details

Granularity auto-selected to evenly cover `PARALLEL_STATEMENTS=N`:

| N | Bucket size | Selector pattern |
|---|-------------|------------------|
| 2 | half-year | `MONTH("dt") IN (1..6)` / `(7..12)` |
| 3 | tri-mester | `MONTH("dt") IN (1..4)` / `(5..8)` / `(9..12)` |
| 4 | quarter | `MONTH("dt") IN (1,2,3)` ... `(10,11,12)` |
| 6 | bimonth | `MONTH("dt") IN (1,2)` ... `(11,12)` |
| 12 | month | `MONTH("dt") = k` |
| ≤ 31 | day-of-month | `DAY("dt") = k` (with bucket N-1 catching k≥N) |
| > 31 | year × month | `(YEAR("dt")*12 + MONTH("dt")) MOD N = k` |

Per-source dialect template (add to dispatch table alongside row-count SQL):

| Source | MONTH fn | DAY fn |
|---|---|---|
| Oracle / Postgres / Redshift / BigQuery / Teradata | `EXTRACT(MONTH FROM "dt")` | `EXTRACT(DAY FROM "dt")` |
| MySQL / MariaDB / Snowflake / Vertica / DB2 / HANA / Databricks | `MONTH("dt")` | `DAY("dt")` |
| SQL Server / Azure SQL | `DATEPART(month, "dt")` | `DATEPART(day, "dt")` |

NULL handling: append `OR "dt" IS NULL` to bucket 0.
TZ handling: timestamp-with-zone normalize to UTC at adapter level; document; offer `PARALLEL_SPLIT=DATE:<col>:UTC` override.

### `PARALLEL_STATEMENTS=AUTO` ceiling

`PARALLEL_STATEMENTS` accepts an integer or the literal `AUTO`:

- explicit int N → use N (no ceiling; user knows their source)
- `AUTO` → heuristic: `min(12, max(1, ceil(row_count / 5_000_000)))` per table, using the row-count already fetched for the threshold gate
- ceiling = **12** (rationale below)
- `1` from heuristic → emits SINGLE (same as below-threshold collapse)

Rationale for ceiling = 12:
- Source-side complaint surface: every IMPORT STATEMENT opens its own JDBC connection. >12 concurrent reads on a transactional OLTP source (Postgres / MySQL / MSSQL) saturates connection pools and triggers DBA paging.
- Snowflake / BigQuery slot-based engines absorb more, but Exasol-side ingest concurrency past 12 yields diminishing returns vs network bandwidth.
- 12 = clean fit with `MONTH`-grain date bucketing (one stmt per month).
- Users with high-CPU Exasol + warehouse sources can bypass: `PARALLEL_STATEMENTS=32` (or whatever).

Audit row should record both the requested value and the effective value (`PARALLEL_STATEMENTS=AUTO -> 8` etc.).

## Two axes of work

### Axis A: Align Oracle adapter with the cross-cutting hierarchy
Current implementation (`oracle_to_exasol.sql:142-298`):
1. Probe `all_tab_partitions` for partitioned tables. If partitions exist, run per-partition `count(*)` IMPORT, bin-pack partitions across `PARALLEL_STATEMENTS` bins.
2. If no partitions, fall back to `ora_hash(rowid) MOD N = i` per statement.

Maps to hierarchy steps **1** (PARTITION) → jumps straight to **6** (ROWID-equivalent via `ora_hash(rowid)`). Steps 2-5 missing. Improvements:
- **Step 2 (PK_RANGE)**: numeric singleton PK detected via `all_constraints` / `all_cons_columns` → emit `WHERE pk BETWEEN lo AND hi` ranges. Min/max from `dba_tab_statistics`.
- **Step 4 (DATE_BUCKET)**: detect first DATE col via `all_tab_columns`; emit `EXTRACT(MONTH FROM "dt")` buckets per `PARALLEL_STATEMENTS`.
- **Step 5 (HASH_NUM)**: first numeric column hash modulo via `ORA_HASH("col", N-1)`.
- **Step 6 (ROWID)**: keep existing `ora_hash(rowid) MOD N`, demoted to last resort.
- **Subpartition awareness** (step 1 enhancement): today's bin-pack ignores `all_tab_subpartitions`. Treat sub-partitions as leaves when present.
- **Histogram-informed bin-pack** (step 1 enhancement): use `dba_tab_col_statistics.histogram` to detect heavy skew on the partition key, fall back to row-count bin-pack only when histograms are flat.

### Axis B: Port parallel emission to adapters that lack it
Each adapter follows the **same cross-cutting hierarchy**. Per-source notes capture engine-specific selector syntax and metadata sources only.

Every adapter takes `PARALLEL_STATEMENTS` (int or `AUTO`) + inherits `PARALLEL_SPLIT` from OPTIONS via dispatcher passthrough. Adapter only chooses the selector syntax per step.

#### B1. PostgreSQL (`postgres_to_exasol.sql`)
| Step | Selector | Metadata |
|---|---|---|
| 1 PARTITION | child table per `pg_inherits` row | `pg_partitioned_table` / `pg_inherits` |
| 2 PK_RANGE | `WHERE "pk" BETWEEN lo AND hi` | `pg_constraint.contype='p'`, `pg_stats` for min/max |
| 3 UNIQUE_NUM | same as PK | `pg_index.indisunique` |
| 4 DATE_BUCKET | `extract(month from "dt")` | `pg_attribute` typename in `('date','timestamp','timestamptz')` |
| 5 HASH_NUM | `MOD(abs(hashtext("col"::text)), N) = k` | `pg_attribute` numeric typename |
| 6 ROWID | `ctid >= '(N,0)' and ctid < '(M,0)'` (heap only) | `pg_class.relpages` |

Row count for threshold gate: `pg_class.reltuples::bigint` (already wired in Speq 1).

#### B2. SQL Server / Azure SQL (`sqlserver_to_exasol.sql`, alias)
| Step | Selector | Metadata |
|---|---|---|
| 1 PARTITION | `$PARTITION.<fn>("col") = p` per partition | `sys.partitions` × `sys.indexes` × `sys.partition_schemes` |
| 2 PK_RANGE | `WHERE "pk" BETWEEN lo AND hi` | `sys.key_constraints type='PK'`, `sys.stats` |
| 4 DATE_BUCKET | `DATEPART(month, "dt")` | `sys.columns.system_type_id IN (40,41,42,43,58,61)` |
| 5 HASH_NUM | `ABS(CHECKSUM("col")) % N = k` | `sys.columns` numeric types |
| 6 ROWID | `convert(varbinary(8), %%physloc%%)` ranges (heap + clustered) | `sys.partitions.partition_id` |

Row count: `sys.dm_db_partition_stats.row_count` (already wired Speq 1).

#### B3. Snowflake (`snowflake_to_exasol.sql`)
| Step | Selector | Metadata |
|---|---|---|
| 1 PARTITION | n/a (micro-partitions opaque) | — |
| 2 PK_RANGE | `WHERE "pk" BETWEEN lo AND hi` | `INFORMATION_SCHEMA.TABLE_CONSTRAINTS` (not enforced; informational only) |
| 4 DATE_BUCKET | `MONTH("dt")` | `INFORMATION_SCHEMA.COLUMNS data_type LIKE 'TIMESTAMP%' OR 'DATE'` |
| 5 HASH_NUM | `MOD(HASH("col"), N) = k` | `INFORMATION_SCHEMA.COLUMNS` numeric types |
| 6 ROWID | n/a → fall to 5 or SINGLE | — |

Row count: `INFORMATION_SCHEMA.TABLES.ROW_COUNT` (exact, free).

#### B4. BigQuery (`bigquery_to_exasol.sql`)
| Step | Selector | Metadata |
|---|---|---|
| 1 PARTITION | `_PARTITIONDATE` ranges (time-partitioned) or partition value bands (range-partitioned) | `INFORMATION_SCHEMA.PARTITIONS`, `INFORMATION_SCHEMA.TABLES.PARTITIONING_TYPE` |
| 2 PK_RANGE | `WHERE "pk" BETWEEN lo AND hi` (informational PK only) | `INFORMATION_SCHEMA.TABLE_CONSTRAINTS` |
| 4 DATE_BUCKET | `EXTRACT(MONTH FROM "dt")` | `INFORMATION_SCHEMA.COLUMNS data_type IN ('DATE','DATETIME','TIMESTAMP')` |
| 5 HASH_NUM | `MOD(FARM_FINGERPRINT(CAST("col" AS STRING)), N) = k` | `INFORMATION_SCHEMA.COLUMNS` numeric types |
| 6 ROWID | n/a → fall to 5 or SINGLE | — |

Row count: revisit `INFORMATION_SCHEMA.PARTITIONS` aggregate so threshold-gate flips BIGQUERY from `skip_with_info` to `sql` mode at the same time.

#### B5. MySQL / MariaDB (`mysql_to_exasol.sql`, `mariadb_to_exasol.sql`)
| Step | Selector | Metadata |
|---|---|---|
| 1 PARTITION | `PARTITION (p)` selector in SELECT | `information_schema.PARTITIONS` |
| 2 PK_RANGE | `WHERE "pk" BETWEEN lo AND hi` | `information_schema.STATISTICS index_name='PRIMARY'` |
| 4 DATE_BUCKET | `MONTH("dt")` | `information_schema.COLUMNS data_type IN ('date','datetime','timestamp')` |
| 5 HASH_NUM | `MOD(CRC32("col"), N) = k` | `information_schema.COLUMNS` numeric types |
| 6 ROWID | not exposed without PK → fall to SINGLE + INFO | — |

Row count: `information_schema.TABLES.TABLE_ROWS` (already wired Speq 1).

#### B6. Redshift (`redshift_to_exasol.sql`)
| Step | Selector | Metadata |
|---|---|---|
| 1 PARTITION | n/a (Redshift has no user-defined partitions) | — |
| 2 PK_RANGE | `WHERE "pk" BETWEEN lo AND hi` (PK informational only) | `pg_constraint` (Redshift compat catalog) |
| 4 DATE_BUCKET | `extract(month from "dt")` | `pg_attribute` date types |
| 5 HASH_NUM | `MOD(STRTOL(MD5("col"::varchar),16), N) = k` | `pg_attribute` numeric types |
| 6 ROWID | n/a → SINGLE | — |

Distkey / sortkey detection (special-case for step 5 acceleration): `svv_table_info.distkey` / `sortkey1` — prefer hash on these cols when present.

Row count: `svv_table_info.tbl_rows` (already wired Speq 1).

#### B7-B12. Lower-priority sources
Vertica, Teradata, DB2, Trino, Dremio, ClickHouse, Databricks, DuckDB, StarRocks, Vectorwise, SAP HANA — defer to follow-up speqs each. None are blockers for v1 of this speq.

### Recommended sequencing
1. Axis A first (Oracle improvements) — same adapter we already understand, lowest risk.
2. Axis B1 (PostgreSQL) — highest-value port, cleanest catalog.
3. Axis B2 (SQL Server) — second-highest usage in customer demos.
4. Axis B3-B5 (Snowflake, BigQuery, MySQL/MariaDB) — round out the top tier.
5. B6+ as separate per-source mini-speqs.

Each adapter port is its own commit on its own branch. Do **not** bundle them — review surface explodes.

## Audit-table additions (incremental over Speq 1's)
Speq 1 did **not** add audit columns (deferred to a separate plan per its own §Non-goals). This speq introduces them in one shot:
- `SPLIT_STRATEGY VARCHAR(32)` — one of `PARTITION` / `PK_RANGE` / `PK_MOD` / `UNIQUE_NUM` / `DATE_BUCKET` / `HASH_NUM` / `ROWID` / `SINGLE`
- `SPLIT_KEY VARCHAR(256)` — the column / pseudo-column / partition name used (`"CREATED_AT"`, `ROWID`, `ctid`, `p_2024_q1`, etc.)
- `PARALLEL_REQUESTED VARCHAR(16)` — raw `PARALLEL_STATEMENTS` OPTIONS value (`AUTO`, `8`, etc.)
- `PARALLEL_EFFECTIVE DECIMAL(4,0)` — number of `STATEMENT` clauses actually emitted (post-AUTO heuristic, post-threshold-gate collapse)

Four new columns. Users query the audit table to compare strategies across migrations and identify where the planner picked sub-optimal strategies.

## Non-goals
- **NOT** changing the threshold-gate mechanic from Speq 1. This speq layers on top.
- **NOT** introducing concurrent execution of IMPORTs across different tables (inter-table parallelism). Each IMPORT remains its own serial statement; this speq only changes what's emitted *inside* one IMPORT.
- **NOT** writing a partition planner that picks strategy automatically based on source-side stats. v1 picks strategy by adapter type + presence of native partitions. Smarter heuristics are a follow-up.

## Test plan

### Tier 1 — Lua unit (per adapter)
Extend `test/test_<source>_to_exasol.lua`. Fixtures per adapter (≥ 6 cases):
1. **partitioned table** → expect step 1 (`SPLIT_STRATEGY=PARTITION`) with N stmts ≈ N partitions
2. **non-partitioned + numeric PK** → expect step 2 (`PK_RANGE`)
3. **no PK + date column named `created_at`** → expect step 4 (`DATE_BUCKET`), grain auto-selected per N
4. **no PK + no date + numeric col** → expect step 5 (`HASH_NUM`)
5. **no PK + no date + no numeric** → expect step 6 (ROWID if engine supports) or step 7 (SINGLE + INFO)
6. **`PARALLEL_SPLIT=DATE:CREATED_AT:QUARTER` override** → expect step 4 with explicit grain ignoring auto-pick
7. **`PARALLEL_STATEMENTS=AUTO` with row_count fixture** → expect heuristic = `min(12, ceil(rows/5M))`, capped at 12
8. **`PARALLEL_STATEMENTS=24` (explicit > ceiling)** → expect 24 stmts (no cap on explicit)

Mock pquery returns metadata + row-count rows; assertions on emitted IMPORT SQL (statement count, WHERE clause shape, SPLIT_STRATEGY audit column).

### Tier 2 — live smoke (per adapter)
One `_reference/smoke_parallel_<source>.py` per adapter (pattern of `_reference/smoke_parallel_row_threshold.py`):
- container boot (Oracle, PG, MySQL, MSSQL, Vertica, DB2 community, HANA Express, Databricks via dbx-runtime image)
- live-account-gated env var (Snowflake, BigQuery, Redshift) — skip if env not set
- 4 fixtures: partitioned + numeric-PK + dated + plain
- run preview with `PARALLEL_STATEMENTS=4;PARALLEL_ROW_THRESHOLD=100`; assert each fixture lands on expected strategy
- run preview with `PARALLEL_STATEMENTS=AUTO`; assert ceiling respected
- execute mode end-to-end for at least one fixture per adapter to validate IMPORT actually loads

### Tier 0 — dispatcher (cross-cutting)
Add cases in `test/test_migrate_to_exasol.lua`:
- `PARALLEL_STATEMENTS=AUTO` resolves to ceiling-bounded heuristic
- `PARALLEL_SPLIT=date:CREATED_AT:MONTH` forwarded to adapter signature
- audit row carries `SPLIT_STRATEGY` + `PARALLEL_REQUESTED` + `PARALLEL_EFFECTIVE` columns

## Estimated effort
- Axis A (Oracle improvements): 1-2 implementer-days.
- Axis B1 (PG): 1-2 implementer-days plus 1 day for live smoke + driver-quirk wrangling.
- Each subsequent adapter: ~1 implementer-day if catalog is well-documented, more if quirks.
- Total to ship top-5 sources (Oracle improvements + PG + SQLServer + Snowflake + BigQuery + MySQL): roughly 2 implementer-weeks.

## Open questions
1. ~~Should `PARALLEL_STATEMENTS` parameter be exposed on every adapter~~ → **resolved**: yes, via cross-cutting `PARALLEL_STATEMENTS=AUTO` + `PARALLEL_SPLIT=AUTO` defaults; adapter without a parallel mechanism falls to SINGLE + INFO row.
2. ~~How to communicate strategy choice to the user~~ → **resolved**: audit columns `SPLIT_STRATEGY` / `SPLIT_KEY` / `PARALLEL_REQUESTED` / `PARALLEL_EFFECTIVE`.
3. Driver-side parallelism vs server-side parallelism: some sources (BigQuery's StorageRead API, Snowflake's Arrow result format) parallelize result-set fetch on the driver side. Does emitting multi-statement on top of that help, hurt, or duplicate work? Bench on a per-source basis before assuming. **AUTO ceiling of 12 mitigates worst case** — even if multi-stmt is redundant with driver parallelism, 12 concurrent JDBC sessions won't overwhelm the source.
4. NDV validation cost: step 4 (DATE_BUCKET) detection benefits from sampling `count(distinct MONTH("dt"))` to confirm even split — but that's an extra round-trip per table. v1 should skip validation and trust the planner; ship NDV-validation as a follow-up speq if real-world skew bites.
5. AUTO ceiling = 12 is a default. Should it be tunable via OPTIONS (`PARALLEL_AUTO_CEILING=24`)? Decide before v1 ships — recommend yes, but with documentation warning users about source-side connection saturation.
