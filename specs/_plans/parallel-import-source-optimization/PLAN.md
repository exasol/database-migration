# Plan: Per-source parallel IMPORT optimization (dispatcher-only)

## Status

Drafted 2026-05-17. **Not started.** Sibling `parallel-row-threshold-dispatcher-gate/` shipped 2026-05-17 (commit `928d30c` on `feat/parallel-row-threshold`).

This plan supersedes earlier per-adapter Axis A / Axis B drafts. Final design lives entirely in `migrate_to_exasol.sql`. Adapter scripts (`oracle_to_exasol.sql`, `postgres_to_exasol.sql`, etc.) are **never touched** — they remain authoritative for source-specific schema discovery, type mapping, identifier quoting, DDL generation, and IMPORT shape. The master script wraps them with a cross-cutting parallel-expansion pass, exactly the way Speq 1's gate wraps them with a threshold-collapse pass.

## Summary

`MIGRATE_TO_EXASOL` adds a second post-processing transform — `transform_for_split` — that sits next to the existing `transform_for_gate`. For every single-statement `IMPORT INTO ...` row the adapter emits whose source row count is **at or above** `PARALLEL_ROW_THRESHOLD`, the dispatcher walks a configurable split-strategy hierarchy, fetches the necessary source-side metadata in a single `IMPORT FROM JDBC` round-trip, and rewrites the IMPORT into N parallel `STATEMENT '...'` clauses with WHERE selectors that the source engine can push down.

Multi-statement IMPORTs (today: only Oracle adapter emits these) pass through unchanged — the source-specific adapter already made an informed split decision; the dispatcher does not second-guess it. The threshold gate continues to collapse multi-stmt IMPORTs back to single when row count is below the threshold.

Result: every adapter that emits single-statement IMPORTs today (Postgres, MySQL/MariaDB, SQL Server/Azure SQL, Snowflake, BigQuery, Redshift, Vertica, DB2, HANA, Netezza, Teradata, Databricks, Dremio, ClickHouse, DuckDB, Starrocks, Trino, Vectorwise, plus any new adapter added later) gains parallel emission automatically through `MIGRATE_TO_EXASOL` — without a line of code touching their adapter scripts.

## Design

### Context

- The dispatcher pattern (master script post-processes adapter output) is validated by Speq 1. The gate collapses multi → single below threshold. Speq 2 introduces the inverse axis: expand single → multi above threshold.
- The SE team and existing customers call adapter scripts directly today (`EXECUTE SCRIPT database_migration.POSTGRES_TO_EXASOL(...)`). That entry point must keep working byte-for-byte. The master script is purely additive: an optional smarter front door.
- The dispatcher already has a per-source dispatch table (`ROW_COUNT_SQL_BY_SOURCE`) introduced in Speq 1. Extending it to a richer `SOURCE_METADATA_BY_SOURCE` covers all needs of Speq 2 (PK col, date col, partition info, first numeric col) plus future speqs.
- Goal: **zero net change to any adapter script.** Master script grows; adapters frozen.

Goals
- A single `transform_for_split` post-processor in `migrate_to_exasol.sql` that operates uniformly on every source.
- A cross-cutting split-strategy hierarchy that picks the best parallel selector per table without per-source code branches.
- `PARALLEL_STATEMENTS=AUTO` mode with a sensible default ceiling (12) to spare OLTP source connection pools.
- Audit columns that record what the dispatcher chose so operators can post-hoc analyze migrations.
- A test surface that fully mocks adapter output + source-metadata round-trip so 99% of coverage runs without a live source DB.

Non-Goals
- Not touching any `<source>_to_exasol.sql` adapter — they stay frozen at current shape forever.
- Not adding new audit columns to the existing output schema beyond the four enumerated below.
- Not introducing inter-table parallelism (one IMPORT at a time remains the contract).
- Not adding a smart planner that picks `PARALLEL_STATEMENTS` per-table; AUTO is a coarse heuristic only.
- Not modifying Speq 1's threshold-gate semantics. The gate runs first; the splitter runs only on rows the gate left as single-stmt.

### Decision

#### Architecture

```
caller
  └─ EXECUTE SCRIPT database_migration.MIGRATE_TO_EXASOL('PG', conn, ..., OPTIONS)
       │
       ├─ master parses OPTIONS
       ├─ master executes adapter: EXECUTE SCRIPT database_migration.POSTGRES_TO_EXASOL(...)
       │     └─ adapter returns rows of generated SQL (DDL + IMPORTs, all single-statement)
       │
       ├─ master post-processes rows:
       │     ├─ transform_for_metadata   ← NEW. ONE source-side round-trip per migration,
       │     │                              fetches (schema, table, row_count, pk_col, pk_type,
       │     │                              date_col, first_numeric_col, partition_info) for
       │     │                              every IMPORT seen in adapter output.
       │     │
       │     ├─ transform_for_gate        ← Speq 1 (shipped). Collapses multi-stmt IMPORTs
       │     │                              whose source rows are below PARALLEL_ROW_THRESHOLD.
       │     │
       │     ├─ transform_for_split       ← NEW. For each *single-stmt* IMPORT whose source
       │     │                              rows are at-or-above threshold, walks the split
       │     │                              hierarchy and rewrites into N STATEMENT clauses.
       │     │
       │     └─ transform_for_audit       ← NEW. Populates SPLIT_STRATEGY / SPLIT_KEY /
       │                                    PARALLEL_REQUESTED / PARALLEL_EFFECTIVE audit cols.
       │
       └─ master returns rows (DEBUG=TRUE) OR executes (DEBUG=FALSE)
```

#### Order of transforms (critical)

1. **`transform_for_metadata`** runs first. Caches per-(schema, table) metadata into a Lua lookup. Both downstream transforms reuse it (one round-trip, not two).
2. **`transform_for_gate`** runs second (unchanged from Speq 1 except it now reads row counts from the cache, not from a fresh lookup).
3. **`transform_for_split`** runs third. Only acts on rows still single-stmt after the gate.
4. **`transform_for_audit`** runs last. Decorates each row's audit-column values based on what gate + split did.

#### Per-table decision tree

For each IMPORT row in adapter output:

```
                       ┌────────────────────────────┐
                       │  STATEMENT clause count?    │
                       └─────────────┬──────────────┘
                          1          │          ≥2
                ┌─────────────────┐  │  ┌──────────────────────────┐
                │ single-stmt     │  │  │ multi-stmt (Oracle today) │
                └────────┬────────┘  │  └────────────┬─────────────┘
                         │           │               │
                         ▼           │               ▼
              ┌──────────────────┐   │   ┌────────────────────────┐
              │ row_count ≥      │   │   │ row_count < threshold? │
              │ threshold?        │   │   │  yes → transform_for_  │
              │  no  → leave      │   │   │  gate collapses to 1   │
              │  yes → expand     │   │   │  no  → pass through    │
              └──────────────────┘   │   └────────────────────────┘
                         │
                         ▼
              ┌──────────────────────┐
              │ walk split hierarchy │
              │ + rewrite into N     │
              │ STATEMENT clauses    │
              └──────────────────────┘
```

Multi-stmt IMPORTs are **never** expanded. Adapters that emit multi-stmt today (Oracle) own that decision; the dispatcher trusts them.

#### Split-strategy hierarchy

Walked per IMPORT, stops at first hit:

| # | Strategy | Selector key | Why this position |
|---|----------|--------------|-------------------|
| 1 | `PARTITION` — per native partition | source partition metadata view | Engine-validated even distribution; pushdown-perfect. **Skipped** in v1 — adapters that have partitioned tables typically already emit multi-stmt; revisit in a follow-up speq if any single-stmt adapter starts shipping with partition metadata. |
| 2 | `PK_RANGE` — `WHERE "pk" BETWEEN lo AND hi` | declared numeric singleton primary key | Sequential IDs → uniform split; cheapest WHERE; engine uses index. |
| 3 | `UNIQUE_NUM` — same shape as PK_RANGE | numeric singleton unique index | Semantic equivalent to PK when PK absent. |
| 4 | `DATE_BUCKET` — `<dialect MONTH fn>("dt") IN (...)` | first DATE/TIMESTAMP col matching `*(_)?(date\|dt\|time\|day\|created\|loaded\|event\|posted)$` (case-insensitive), else first DATE/TIMESTAMP by ordinal | Pushdown-friendly; readable in audit; cheaper than hash; resilient where PK is composite / UUID. |
| 5 | `HASH_NUM` — `MOD(<dialect hash fn>("col"), N) = k` | first numeric not-null column | Catches integer surrogate cols not flagged as PK. |
| 6 | `ROWID` — engine-specific physical row identifier ranges | dialect (`ctid` for PG, `%%physloc%%` for MSSQL, `RID_BIT` for DB2) | Always works on heap; zero metadata needed. v1 ships PG + MSSQL + DB2 only. |
| 7 | `SINGLE` — emit one STATEMENT + `INFO` row | — | Safe fallback. |

`PARALLEL_SPLIT` OPTIONS knob (per migration; not per table):

- `AUTO` (default) — walk hierarchy
- `PARTITION` — force step 1; fail-soft to SINGLE if no partition metadata
- `PK` — force step 2 (or step 3 if no PK); fail-soft to SINGLE
- `DATE` — force step 4; auto-pick column
- `DATE:<col>` — force step 4 with named column
- `DATE:<col>:<grain>` — force step 4 with explicit grain (`MONTH` / `QUARTER` / `HALF` / `DAY` / `YEAR_MONTH`)
- `HASH:<col>` — force step 5 with named column
- `ROWID` — force step 6
- `OFF` — force SINGLE (equivalent to `PARALLEL_STATEMENTS=1`)

#### Date-bucket details

Granularity auto-selected per `PARALLEL_STATEMENTS=N`:

| N | Bucket | Selector pattern |
|---|--------|------------------|
| 2 | half-year | `MONTH("dt") IN (1..6)` / `(7..12)` |
| 3 | tri-mester | `MONTH("dt") IN (1..4)` / `(5..8)` / `(9..12)` |
| 4 | quarter | `MONTH("dt") IN (1,2,3)` ... `(10,11,12)` |
| 6 | bimonth | `MONTH("dt") IN (1,2)` ... `(11,12)` |
| 12 | month | `MONTH("dt") = k` |
| ≤ 31 | day-of-month | `DAY("dt") = k` (bucket N-1 catches `k ≥ N`) |
| > 31 | year × month | `(YEAR("dt")*12 + MONTH("dt")) MOD N = k` |

NULL handling: append `OR "dt" IS NULL` to bucket 0.
Time-zone: TIMESTAMP-WITH-ZONE values normalized to UTC at the dispatcher layer via the source-dialect cast. Document; override via `PARALLEL_SPLIT=DATE:<col>:UTC`.

#### `PARALLEL_STATEMENTS=AUTO` resolution

`PARALLEL_STATEMENTS` accepts an integer or the literal `AUTO`:

- explicit int `N` → use N (no ceiling; user knows their source)
- `AUTO` (default) → per-table heuristic: `min(12, max(1, ceil(row_count / 5_000_000)))`
- ceiling = **12** (rationale: >12 concurrent JDBC sessions saturate OLTP source connection pools; 12 fits MONTH-grain bucketing cleanly; high-CPU sources can bypass via explicit `PARALLEL_STATEMENTS=24` or similar)
- `1` from heuristic → behaves identically to threshold-collapsed single-stmt

`AUTO` ceiling is itself a `PARALLEL_AUTO_CEILING` OPTIONS knob (default 12) so high-end Exasol clusters with high-end source DBs can raise the auto-cap without losing the AUTO heuristic shape.

#### Per-source metadata SQL (`SOURCE_METADATA_BY_SOURCE`)

One round-trip per migration. The dispatcher builds a single `IMPORT FROM JDBC AT <CONN> statement '<per-source SQL>'` that returns, for every IMPORT seen in adapter output, a row of:

```
src_schema   VARCHAR(...)
src_table    VARCHAR(...)
src_rows     DECIMAL(36,0)        -- already wired in Speq 1
src_pk_col   VARCHAR(...)         -- single-col numeric PK if any
src_pk_type  VARCHAR(...)
src_date_col VARCHAR(...)         -- best DATE/TIMESTAMP col by heuristic
src_num_col  VARCHAR(...)         -- first numeric not-null col (excluding PK)
src_partitioned  BOOLEAN          -- has any partition metadata?
```

Per-source SQL strings live in a single dispatch table inside `migrate_to_exasol.sql`. Extending `ROW_COUNT_SQL_BY_SOURCE` into this richer schema is one of the v1 implementation tasks. Sources that cannot report all columns (e.g. Databricks: no row count) emit NULL for the missing fields; the hierarchy walker treats NULL as a step-skip signal.

| Source | Metadata SQL sketch | Notes |
|--------|---------------------|-------|
| ORACLE | `dba_tables` + `dba_constraints` + `dba_tab_columns` joined on owner.table | Numeric PK detection via `dba_constraints.constraint_type='P'` + `dba_cons_columns`. |
| POSTGRES | `pg_class` + `pg_namespace` + `pg_constraint` + `pg_attribute` joined | Date col via `pg_attribute.atttypid IN (date, timestamp, timestamptz)`. |
| MYSQL / MARIADB | `information_schema.tables` + `key_column_usage` + `columns` | Identical SQL works for both. |
| SQLSERVER / AZURE_SQL | `sys.tables` + `sys.indexes` + `sys.key_constraints` + `sys.columns` | Same SQL across both. |
| SNOWFLAKE | `INFORMATION_SCHEMA.TABLES` + `TABLE_CONSTRAINTS` + `COLUMNS` | Constraints informational only. |
| BIGQUERY | `INFORMATION_SCHEMA.TABLES` + `COLUMNS` (per-dataset) | Lifts BQ from Speq 1's `skip_with_info` to fully supported in v1 of this speq — splitter falls through gracefully when partition / PK info is absent. |
| REDSHIFT | `svv_table_info` + `pg_constraint` + `pg_attribute` | Reuse Speq 1's row-count source. |
| VERTICA | `tables` + `primary_keys` + `columns` | |
| DB2 | `syscat.tables` + `tabconst` + `columns` | |
| HANA | `sys.m_tables` + `sys.indexes` + `sys.table_columns` | |
| NETEZZA / TERADATA | catalog views; mock-only test coverage | |
| DATABRICKS | `information_schema.tables` + `columns`; row_count NULL | Row count permanently NULL → splitter never fires; gate already documents NULL → single. |

If the metadata round-trip fails for any reason, `transform_for_metadata` populates the cache with all-NULL rows and logs an `INFO` row. Downstream transforms see NULL everywhere → behave conservatively (gate skipped, splitter skipped). Migration continues with single-statement IMPORTs.

#### Audit columns

Four new columns appended to the existing output schema:

- `SPLIT_STRATEGY VARCHAR(32)` — one of `PARTITION` / `PK_RANGE` / `UNIQUE_NUM` / `DATE_BUCKET` / `HASH_NUM` / `ROWID` / `SINGLE` / `MULTI_PASSTHROUGH`
- `SPLIT_KEY VARCHAR(256)` — column / pseudo-column / partition name actually used (`CREATED_AT`, `ctid`, `RID_BIT`, `p_2024_q1`)
- `PARALLEL_REQUESTED VARCHAR(16)` — raw `PARALLEL_STATEMENTS` OPTIONS value (`AUTO`, `8`, etc.)
- `PARALLEL_EFFECTIVE DECIMAL(4,0)` — number of `STATEMENT` clauses actually emitted after AUTO heuristic + gate collapse

These columns are populated by `transform_for_audit` for every row, not just IMPORTs. Non-IMPORT rows get `NULL` in all four. Adding them to the output schema is a breaking change to the audit-table column contract; document in CHANGELOG and bump the example schema in any reference docs.

### Patterns

| Pattern | Where | Why |
|---------|-------|-----|
| Per-source dispatch table | `SOURCE_METADATA_BY_SOURCE` extending `ROW_COUNT_SQL_BY_SOURCE` | One place to add new sources, mirrors Speq 1's pattern. |
| Single source-side round-trip | `transform_for_metadata` builds one IMPORT FROM JDBC per migration with OR-predicate of all needed `(schema, table)` pairs | Adapters that never emit parallel pay only the metadata round-trip cost. |
| Lazy hierarchy walk | `transform_for_split` walks per-table, stops at first hit | No wasted metadata fetches; falls through gracefully on NULLs. |
| Soft-fail | Any error in metadata fetch or rewrite → emit INFO row and leave IMPORTs unchanged | The splitter is an optimization; a metadata outage must never break a migration. |
| Dialect-template table | `<source>_DIALECT = { month_fn = "EXTRACT(MONTH FROM %s)", hash_fn = "MOD(MD5(%s::TEXT), %d) = %d", ... }` inside the master | One place to add new sources; templates are pure data. |
| Adapter contract immutable | Adapters are invoked exactly as today via `EXECUTE SCRIPT database_migration.<SRC>_TO_EXASOL(...)`; their output rows are post-processed but their source code is not modified | Holy-grail invariant: master grows, adapters frozen. |

### Consequences

| Decision | Alternatives Considered | Rationale |
|----------|------------------------|-----------|
| Dispatcher-only implementation | (a) per-adapter parallel emission ports; (b) external orchestrator script | The dispatcher pattern is validated by Speq 1. Per-adapter ports duplicate logic 18 times; external orchestrator forks the entry-point story. |
| Adapter scripts frozen forever | Edit each adapter to expose richer signatures | Holy-grail principle: master grows, adapters do not. Existing direct adapter callers (SE team, customers) keep working byte-for-byte. |
| `PARALLEL_STATEMENTS=AUTO` default | Default to `1` (off); default to explicit `4` | AUTO is the friendliest default — gives users the win without configuration; explicit overrides remain trivial. |
| AUTO ceiling = 12 (tunable via `PARALLEL_AUTO_CEILING`) | Hard 12; hard 8; per-source cap | 12 covers MONTH-grain neatly + spares OLTP connection pools; high-end users tune up. |
| Source-side metadata via one round-trip | Per-table round-trips; static metadata cache | One round-trip is cheap; static cache stale-ness defeats correctness. |
| `transform_for_split` runs only on single-stmt IMPORTs | Always run, second-guess adapter | Trust the adapter's parallel decision when it made one. Avoids double-rewrite confusion for Oracle. |
| Skip step 1 (PARTITION) in v1 | Implement partition-aware splitting in v1 | Partitioned tables are typically already handled by adapters that emit multi-stmt; v1 ships steps 2-7 only, partition support is a follow-up speq. |

## Features

| Feature | Status | Spec |
|---------|--------|------|
| parallel-split-dispatcher | NEW | `migrate-to-exasol/parallel-split-dispatcher/spec.md` |
| source-metadata-roundtrip | NEW | `migrate-to-exasol/source-metadata-roundtrip/spec.md` |
| parallel-auto-ceiling | NEW | `migrate-to-exasol/parallel-auto-ceiling/spec.md` |
| split-audit-columns | NEW | `migrate-to-exasol/split-audit-columns/spec.md` |

(Spec files to be authored as part of Phase 0; see Implementation Tasks below.)

## Dependencies

- **Hard, satisfied:** `parallel-row-threshold-dispatcher-gate/` shipped (commit `928d30c`). The metadata round-trip introduced here subsumes the gate's row-count lookup; the gate gets refactored to read from the shared cache.
- **None on adapter scripts.** All work in `migrate_to_exasol.sql`.

## Migration

| Current (post-Speq-1 state on `feat/parallel-row-threshold`) | After this speq |
|--------------------------------------------------------------|-----------------|
| Dispatcher invokes adapter, runs `transform_for_gate` (one source-side round-trip for row counts), emits normalized rows. | Dispatcher invokes adapter, runs `transform_for_metadata` (one source-side round-trip for full metadata), then `transform_for_gate` (reads from cache), then `transform_for_split` (rewrites single-stmt IMPORTs above threshold), then `transform_for_audit` (populates new cols), then emits rows. |
| `OUT_COLUMNS` schema has 7 columns. | `OUT_COLUMNS` schema has 11 columns: existing 7 plus `SPLIT_STRATEGY`, `SPLIT_KEY`, `PARALLEL_REQUESTED`, `PARALLEL_EFFECTIVE`. |
| OPTIONS: `PARALLEL_ROW_THRESHOLD`, plus adapter-specific knobs (`DB2SCHEMA`, `CATALOG2SCHEMA`, `PROJECT_ID`, `PARALLEL_STATEMENTS=<int>`, etc.). | OPTIONS adds: `PARALLEL_STATEMENTS=AUTO|<int>`, `PARALLEL_SPLIT=AUTO|PK|DATE[...]|HASH[...]|ROWID|OFF`, `PARALLEL_AUTO_CEILING=<int>`. |
| Adapter scripts authoritative for parallel emission decisions per source. | Unchanged — adapters still authoritative for their own multi-stmt emission. The splitter only acts on single-stmt IMPORTs that adapters emit. |
| Tests: 50 cases in `test_migrate_to_exasol.lua`. | Tests: ~80 cases — adds ~30 covering metadata round-trip, splitter, AUTO resolution, audit cols, and edge cases per spec scenarios. |

## Implementation Tasks

### Phase 0 — spec deltas + dispatch tables

1. Author 4 spec files under `specs/_plans/parallel-import-source-optimization/migrate-to-exasol/` matching the Features table.
2. Run `speq plan validate parallel-import-source-optimization` — must validate clean.
3. Extend `ROW_COUNT_SQL_BY_SOURCE` into `SOURCE_METADATA_BY_SOURCE` with the 8-column return schema. Update Speq 1's gate to read row counts from the new richer cache instead of issuing its own query.

### Phase 1 — metadata round-trip

4. Implement `transform_for_metadata(res, source_type, connection_name)` that:
   - parses every IMPORT in `res`, extracts source `(schema, table)` pair from the inner SELECT's `from "X"."Y"` (logic already in Speq 1's gate; refactor into shared helper)
   - builds one `IMPORT FROM JDBC at <CONN> statement '<per-source metadata SQL>'`
   - returns a Lua-table cache keyed by `(schema, table)` with all 8 fields
   - soft-fails into an all-NULL cache + `INFO` row on any error
5. Refactor `transform_for_gate` to consume the cache.

### Phase 2 — splitter + dialect tables

6. Author a `DIALECT_BY_SOURCE` table inside the master. Each entry: `month_fn`, `quarter_fn`, `day_fn`, `year_fn`, `hash_fn`, `rowid_expr`, `rowid_supported (bool)`, `partition_selector_template` (for the deferred step 1).
7. Implement `pick_split_strategy(import_meta, options)` returning a `(strategy, key, hint)` triple by walking the hierarchy in order, respecting OPTIONS overrides.
8. Implement `build_where_for_split(strategy, key, dialect, n, k)` returning the WHERE-clause fragment for stmt `k` of `N`.
9. Implement `rewrite_import_to_multi_stmt(sql, where_per_k)` that takes the original single-stmt IMPORT, extracts its inner SELECT, and emits N STATEMENT clauses by AND-ing each WHERE fragment onto the inner SELECT.
10. Implement `transform_for_split(res, cache, options)` that walks IMPORTs, calls the above helpers per applicable row, replaces the row's SQL.

### Phase 3 — AUTO resolution + audit + wiring

11. Implement `resolve_parallel_statements(options, row_count, ceiling)` returning `(effective_n, requested_value_string)`.
12. Implement `transform_for_audit(res, decisions)` that decorates every row with the four new audit columns. (`decisions` is a per-row record produced cooperatively by gate + splitter — store it in a side table keyed by row index.)
13. Bump `OUT_COLUMNS` to 11 columns. Adjust all paths (`build_row`, `normalize_rows`, `execute_generated_sql`, `transform_for_gate`'s INFO-row constructor) to emit 11-tuple rows.
14. Wire `transform_for_metadata` → `transform_for_gate` → `transform_for_split` → `transform_for_audit` into `execute_adapter` in that order.

### Phase 4 — tests

15. Lua unit tests (extend `test_migrate_to_exasol.lua`):
    - 11 metadata-round-trip scenarios (per `source-metadata-roundtrip` spec)
    - 11 splitter scenarios (per `parallel-split-dispatcher` spec) covering each hierarchy step
    - 7 AUTO-ceiling scenarios (per `parallel-auto-ceiling` spec)
    - 6 audit-column scenarios (per `split-audit-columns` spec)
    - 4 cross-feature integration scenarios (gate + splitter + AUTO + audit together)
    Target: ~40 new test cases bringing total to ~90.
16. Python runtime test additions in `test/test_migrate_to_exasol_runtime.py`: verify the new column count, audit-column population, and AUTO knob propagation.

### Phase 5 — live smoke per source

17. Extend `_reference/smoke_parallel_row_threshold.py` into `_reference/smoke_parallel_split_<source>.py` (one per source). Pattern: spin up source container (or env-gated cloud account), seed `SMALL_T` + `BIG_T` + `DATED_T` + `KEYLESS_T` fixtures, run preview + execute with `PARALLEL_STATEMENTS=AUTO`, assert strategies + statement counts.
18. Tier 0 priority order: Postgres, SQL Server, MySQL, Snowflake, BigQuery. Tier 1: Vertica, DB2, HANA, Databricks. Tier 2 (mock-only): Redshift, Netezza, Teradata, Trino, Dremio, ClickHouse, DuckDB, Starrocks.

### Phase 6 — documentation

19. Update `README.md` section on `MIGRATE_TO_EXASOL` OPTIONS to enumerate the new knobs.
20. Add a section to the wrapper README that explains the master-script architecture: master is a cross-cutting transform engine on top of frozen adapter scripts; adapters can still be invoked directly.
21. Update `specs/mission.md` to canonicalize the "master script as orchestrator + cross-cutting transform engine; adapter scripts frozen" architectural principle.

## Dead Code Removal

None. Adapters frozen; their existing parallel logic (Oracle's `ORA_HASH(rowid)` plus partition bin-pack) is left in place and respected by `transform_for_split`'s "skip if multi-stmt already" rule.

## Verification

### Scenario Coverage

Each scenario below corresponds to a `* GIVEN ... * WHEN ... * THEN ...` block in one of the four spec files. See those spec files for full text; this table summarizes test locations.

| Scenario | Test Type | Test Location | Test Name |
|----------|-----------|---------------|-----------|
| Metadata round-trip fetches per-table fields in one IMPORT FROM JDBC | Integration (Lua) | `test/test_migrate_to_exasol.lua` | `metadata round-trip fires once per migration` |
| Metadata round-trip soft-fails on lookup error | Integration (Lua) | `test/test_migrate_to_exasol.lua` | `metadata round-trip soft-fails to all-NULL cache` |
| Below-threshold single-stmt IMPORT passes through (gate semantics preserved) | Integration (Lua) | `test/test_migrate_to_exasol.lua` | `splitter respects threshold gate decision` |
| At-or-above-threshold single-stmt IMPORT with numeric PK → step 2 (PK_RANGE) | Integration (Lua) | `test/test_migrate_to_exasol.lua` | `splitter picks PK_RANGE on numeric PK` |
| At-or-above-threshold single-stmt IMPORT with no PK + date col → step 4 (DATE_BUCKET) | Integration (Lua) | `test/test_migrate_to_exasol.lua` | `splitter picks DATE_BUCKET on date col` |
| At-or-above-threshold single-stmt IMPORT with no PK + no date + numeric col → step 5 (HASH_NUM) | Integration (Lua) | `test/test_migrate_to_exasol.lua` | `splitter picks HASH_NUM on first numeric col` |
| At-or-above-threshold single-stmt IMPORT with no PK + no date + no numeric + ROWID-supporting source → step 6 | Integration (Lua) | `test/test_migrate_to_exasol.lua` | `splitter picks ROWID on PG ctid` |
| No usable split column → step 7 (SINGLE + INFO row) | Integration (Lua) | `test/test_migrate_to_exasol.lua` | `splitter falls to SINGLE with INFO row` |
| Multi-stmt IMPORT (Oracle today) passes through splitter unchanged | Integration (Lua) | `test/test_migrate_to_exasol.lua` | `splitter respects pre-existing multi-stmt IMPORT` |
| `PARALLEL_SPLIT=OFF` disables splitter regardless of metadata | Integration (Lua) | `test/test_migrate_to_exasol.lua` | `PARALLEL_SPLIT=OFF disables splitter` |
| `PARALLEL_SPLIT=DATE:CREATED_AT:QUARTER` forces step 4 with explicit grain | Integration (Lua) | `test/test_migrate_to_exasol.lua` | `PARALLEL_SPLIT=DATE override picks named col + grain` |
| `PARALLEL_SPLIT=HASH:CUSTOMER_ID` forces step 5 with named col | Integration (Lua) | `test/test_migrate_to_exasol.lua` | `PARALLEL_SPLIT=HASH override picks named col` |
| `PARALLEL_STATEMENTS=AUTO` resolves to ceiling-bounded heuristic | Integration (Lua) | `test/test_migrate_to_exasol.lua` | `AUTO resolves via row_count heuristic capped at 12` |
| `PARALLEL_STATEMENTS=AUTO` with `PARALLEL_AUTO_CEILING=24` | Integration (Lua) | `test/test_migrate_to_exasol.lua` | `PARALLEL_AUTO_CEILING raises AUTO cap` |
| `PARALLEL_STATEMENTS=24` (explicit) bypasses ceiling | Integration (Lua) | `test/test_migrate_to_exasol.lua` | `explicit PARALLEL_STATEMENTS bypasses ceiling` |
| Audit columns populated for IMPORT, DDL, INFO, SUMMARY rows correctly | Integration (Lua) | `test/test_migrate_to_exasol.lua` | `audit columns populated per row kind` |
| Multiple IMPORTs in one migration audited independently | Integration (Lua) | `test/test_migrate_to_exasol.lua` | `audit cols per-IMPORT independent in one migration` |
| Multiple IMPORTs share one metadata round-trip | Integration (Lua) | `test/test_migrate_to_exasol.lua` | `metadata round-trip batches all IMPORTs` |
| Date-bucket NULL handling: bucket 0 includes IS NULL | Integration (Lua) | `test/test_migrate_to_exasol.lua` | `date bucket bucket-0 includes IS NULL` |
| Date-bucket grain auto-selected per N | Integration (Lua) | `test/test_migrate_to_exasol.lua` | `date bucket grain matches PARALLEL_STATEMENTS` |
| Live smoke: Postgres parallel split end-to-end | Manual (Python) | `_reference/smoke_parallel_split_postgres.py` | n/a |
| Live smoke: SQL Server parallel split end-to-end | Manual (Python) | `_reference/smoke_parallel_split_sqlserver.py` | n/a |
| Live smoke: MySQL parallel split end-to-end | Manual (Python) | `_reference/smoke_parallel_split_mysql.py` | n/a |
| Live smoke: Snowflake parallel split end-to-end (env-gated) | Manual (Python) | `_reference/smoke_parallel_split_snowflake.py` | n/a |
| Live smoke: BigQuery parallel split end-to-end (env-gated) | Manual (Python) | `_reference/smoke_parallel_split_bigquery.py` | n/a |

(Spec files in Phase 0 expand each of the first ~20 rows into full GIVEN/WHEN/THEN form.)

### Manual Testing

| Feature | Command | Expected Output |
|---------|---------|-----------------|
| parallel-split-dispatcher | `python3 _reference/smoke_parallel_split_postgres.py` | `SMALL_T` → 1 stmt (gate-collapsed); `BIG_T` (5M rows numeric PK) → 4 stmts (`SPLIT_STRATEGY=PK_RANGE`); `DATED_T` (5M rows, no PK, `created_at`) → 4 stmts (`SPLIT_STRATEGY=DATE_BUCKET`); final `PASS` |
| source-metadata-roundtrip | Same harness — assert exactly one `IMPORT FROM JDBC` lookup query against the source per migration regardless of number of tables | Inline assert in smoke harness |
| parallel-auto-ceiling | `python3 _reference/smoke_parallel_split_postgres.py --auto` | `PARALLEL_STATEMENTS=AUTO` on a seeded 50M-row table → `PARALLEL_EFFECTIVE=10` (50M/5M = 10, under cap 12); same on a seeded 100M-row table → `PARALLEL_EFFECTIVE=12` (capped) |
| split-audit-columns | Inspect the audit output of any smoke run | Four new columns populated per row; non-IMPORT rows have NULL in all four |

### Checklist

| Step | Command | Expected |
|------|---------|----------|
| Build | `python3 -c "import ast, pathlib; [ast.parse(pathlib.Path(p).read_text(), filename=p) for p in ('test/create_script.py','test/export_res.py','test/mock_test.py')]"` | Exit 0 |
| Test (Lua, all adapters) | `for t in test/test_*.lua; do lua "$t" || exit 1; done` | All passing |
| Test (Python runtime) | `python3 test/test_migrate_to_exasol_runtime.py` | Exit 0 |
| Spec validation | `speq plan validate parallel-import-source-optimization` | `Plan ... validation passed`, 0 warnings |
| Live smoke (one Tier-0 source, e.g. Postgres) | `python3 _reference/smoke_parallel_split_postgres.py` | `PASS` |
| Lint | `git diff --check` | No errors |
| Adapter no-touch | `git diff origin/master..HEAD -- '*_to_exasol.sql' | grep -v migrate_to_exasol` | Empty output — no adapter touched by this speq's commits |
