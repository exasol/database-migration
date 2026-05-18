# Feature: parallel-split-dispatcher

The dispatcher (`MIGRATE_TO_EXASOL`) inspects every single-statement IMPORT row that adapters emit, walks a configurable split-strategy hierarchy backed by the shared source-metadata cache, and rewrites those IMPORTs into N parallel `statement '...'` clauses with WHERE selectors that the source engine can push down. The splitter lives entirely inside `migrate_to_exasol.sql`; adapter scripts are not modified, so any current or future adapter that emits single-statement IMPORTs gains parallel emission automatically. Multi-statement IMPORTs (Oracle today) are always passed through — the adapter already made an informed parallel decision.

## Background

* All scenarios use `MIGRATE_TO_EXASOL` as the entry point.
* The splitter runs *after* `transform_for_gate` (Speq 1) and *only* on rows still single-statement at that point.
* `PARALLEL_STATEMENTS` is provided as an `OPTIONS` key. Accepts `AUTO` (default) or a positive integer. `AUTO` resolution is specified in `parallel-auto-ceiling`.
* `PARALLEL_SPLIT` is provided as an `OPTIONS` key. Accepts `AUTO` (default), `PARTITION`, `PK`, `DATE`, `DATE:<col>`, `DATE:<col>:<grain>`, `HASH:<col>`, `ROWID`, `OFF`. Forces a single step of the hierarchy (or disables it via `OFF`).
* `PARALLEL_ROW_THRESHOLD` from Speq 1 still gates whether the splitter fires at all (only rows whose source row count is at-or-above threshold are eligible).
* Per-table metadata (`src_rows`, `src_pk_col`, `src_pk_type`, `src_date_col`, `src_num_col`, `src_partitioned`) is read from the shared metadata cache populated by `transform_for_metadata`. The splitter never issues its own source-side query.
* Split-strategy hierarchy (walked top-to-bottom, stops at first hit):

  1. `PARTITION` — per native source partition. **Skipped in v1.**
  2. `PK_RANGE` — `WHERE "pk" BETWEEN lo AND hi` on a declared numeric singleton primary key.
  3. `UNIQUE_NUM` — same WHERE shape on a numeric singleton unique index when no PK.
  4. `DATE_BUCKET` — dialect-specific MONTH/QUARTER/DAY function `IN (...)` over the best DATE/TIMESTAMP column.
  5. `HASH_NUM` — `MOD(<dialect hash fn>("col"), N) = k` over the first numeric not-null column.
  6. `ROWID` — dialect-specific physical row identifier ranges (`ctid` for PG, `%%physloc%%` for MSSQL, `RID_BIT` for DB2). v1 supports PG + MSSQL + DB2 only.
  7. `SINGLE` — emit one STATEMENT + a `STEP_KIND = INFO` row noting no split column was found.
* Date-bucket grain is auto-selected per `N`: 2 = half-year, 3 = tri-mester, 4 = quarter, 6 = bimonth, 12 = month, ≤31 = day-of-month, >31 = `(YEAR*12 + MONTH) MOD N`.
* NULL handling for `DATE_BUCKET`: bucket `0` appends `OR "dt" IS NULL`.
* When `transform_for_split` cannot rewrite an IMPORT (no usable strategy, dialect missing, helper raises), it MUST leave the row's SQL unchanged, log a `STEP_KIND = INFO` row, and continue with the rest of the migration. The splitter is an optimization; any failure inside it must NOT break a migration.

## Scenarios

### Scenario: Below-threshold single-statement IMPORT passes through

* *GIVEN* an adapter emits a single-statement IMPORT for source `SMOKE.SMALL_T`
* *AND* the metadata cache reports `src_rows = 500000` for `SMOKE.SMALL_T`
* *AND* `OPTIONS` contains `PARALLEL_ROW_THRESHOLD=1000000;PARALLEL_STATEMENTS=AUTO`
* *WHEN* `MIGRATE_TO_EXASOL` is executed
* *THEN* the dispatcher SHALL pass the IMPORT through unchanged
* *AND* the splitter MUST NOT walk the hierarchy for this row
* *AND* the audit row for this IMPORT SHALL carry `SPLIT_STRATEGY = 'SINGLE'`, `PARALLEL_EFFECTIVE = 1`

### Scenario: At-or-above-threshold IMPORT with numeric PK picks PK_RANGE

* *GIVEN* an adapter emits a single-statement IMPORT for source `SMOKE.ORDERS`
* *AND* the metadata cache reports `src_rows = 20000000`, `src_pk_col = 'ORDER_ID'`, `src_pk_type = 'NUMBER'`
* *AND* `OPTIONS` contains `PARALLEL_ROW_THRESHOLD=1000000;PARALLEL_STATEMENTS=4;PARALLEL_SPLIT=AUTO`
* *WHEN* `MIGRATE_TO_EXASOL` is executed
* *THEN* the dispatcher SHALL rewrite the IMPORT into exactly 4 `statement '...'` clauses
* *AND* each clause's inner SELECT MUST AND its source WHERE with a `"ORDER_ID" BETWEEN <lo> AND <hi>` predicate covering disjoint ranges of the PK domain
* *AND* the audit row for this IMPORT SHALL carry `SPLIT_STRATEGY = 'PK_RANGE'`, `SPLIT_KEY = 'ORDER_ID'`, `PARALLEL_EFFECTIVE = 4`

### Scenario: At-or-above-threshold IMPORT with no PK + numeric unique picks UNIQUE_NUM

* *GIVEN* an adapter emits a single-statement IMPORT for source `SMOKE.LEGACY_ORDERS`
* *AND* the metadata cache reports `src_rows = 20000000`, `src_pk_col = NULL`, and (per source-metadata-roundtrip) records a numeric singleton unique index column `LEGACY_ID`
* *AND* `OPTIONS` contains `PARALLEL_ROW_THRESHOLD=1000000;PARALLEL_STATEMENTS=4;PARALLEL_SPLIT=AUTO`
* *WHEN* `MIGRATE_TO_EXASOL` is executed
* *THEN* the dispatcher SHALL rewrite the IMPORT into 4 `statement '...'` clauses using `"LEGACY_ID" BETWEEN <lo> AND <hi>` selectors
* *AND* the audit row for this IMPORT SHALL carry `SPLIT_STRATEGY = 'UNIQUE_NUM'`, `SPLIT_KEY = 'LEGACY_ID'`

### Scenario: At-or-above-threshold IMPORT with date col picks DATE_BUCKET

* *GIVEN* an adapter emits a single-statement IMPORT for source `SMOKE.EVENTS`
* *AND* the metadata cache reports `src_rows = 20000000`, `src_pk_col = NULL`, `src_date_col = 'EVENT_DT'`
* *AND* `OPTIONS` contains `PARALLEL_ROW_THRESHOLD=1000000;PARALLEL_STATEMENTS=4;PARALLEL_SPLIT=AUTO`
* *WHEN* `MIGRATE_TO_EXASOL` is executed
* *THEN* the dispatcher SHALL rewrite the IMPORT into 4 `statement '...'` clauses
* *AND* each clause MUST AND its inner SELECT with a `MONTH("EVENT_DT") IN (...)` predicate spanning a disjoint quarter (`(1,2,3)` / `(4,5,6)` / `(7,8,9)` / `(10,11,12)`)
* *AND* the `(1,2,3)` clause SHALL also include `OR "EVENT_DT" IS NULL`
* *AND* the audit row for this IMPORT SHALL carry `SPLIT_STRATEGY = 'DATE_BUCKET'`, `SPLIT_KEY = 'EVENT_DT'`

### Scenario: At-or-above-threshold IMPORT with first numeric col picks HASH_NUM

* *GIVEN* an adapter emits a single-statement IMPORT for source `SMOKE.SESSIONS`
* *AND* the metadata cache reports `src_rows = 20000000`, `src_pk_col = NULL`, `src_date_col = NULL`, `src_num_col = 'CUSTOMER_ID'`
* *AND* `OPTIONS` contains `PARALLEL_ROW_THRESHOLD=1000000;PARALLEL_STATEMENTS=4;PARALLEL_SPLIT=AUTO`
* *WHEN* `MIGRATE_TO_EXASOL` is executed
* *THEN* the dispatcher SHALL rewrite the IMPORT into 4 `statement '...'` clauses each carrying a `MOD(<dialect hash fn>("CUSTOMER_ID"), 4) = k` predicate for `k` in `{0,1,2,3}`
* *AND* the audit row for this IMPORT SHALL carry `SPLIT_STRATEGY = 'HASH_NUM'`, `SPLIT_KEY = 'CUSTOMER_ID'`

### Scenario: ROWID-supporting source with no other split column picks ROWID

* *GIVEN* the source type is `POSTGRES`
* *AND* an adapter emits a single-statement IMPORT for source `SMOKE.HEAP_T`
* *AND* the metadata cache reports `src_rows = 20000000` and all of `src_pk_col`, `src_date_col`, `src_num_col` are NULL
* *AND* `OPTIONS` contains `PARALLEL_ROW_THRESHOLD=1000000;PARALLEL_STATEMENTS=4;PARALLEL_SPLIT=AUTO`
* *WHEN* `MIGRATE_TO_EXASOL` is executed
* *THEN* the dispatcher SHALL rewrite the IMPORT into 4 `statement '...'` clauses each carrying a `ctid`-range predicate
* *AND* the audit row for this IMPORT SHALL carry `SPLIT_STRATEGY = 'ROWID'`, `SPLIT_KEY = 'ctid'`

### Scenario: No usable split column falls through to SINGLE with INFO row

* *GIVEN* the source type is `SNOWFLAKE` (no ROWID support in v1)
* *AND* an adapter emits a single-statement IMPORT for source `SMOKE.MYSTERY_T`
* *AND* the metadata cache reports `src_rows = 20000000` and all of `src_pk_col`, `src_date_col`, `src_num_col` are NULL
* *AND* `OPTIONS` contains `PARALLEL_ROW_THRESHOLD=1000000;PARALLEL_STATEMENTS=4;PARALLEL_SPLIT=AUTO`
* *WHEN* `MIGRATE_TO_EXASOL` is executed
* *THEN* the dispatcher SHALL pass the IMPORT through unchanged
* *AND* the dispatcher SHALL emit one extra row with `STEP_KIND = 'INFO'` describing that no split column was available for `SMOKE.MYSTERY_T`
* *AND* the audit row for the IMPORT SHALL carry `SPLIT_STRATEGY = 'SINGLE'`, `SPLIT_KEY = NULL`, `PARALLEL_EFFECTIVE = 1`

### Scenario: Multi-statement IMPORT (Oracle today) passes through splitter unchanged

* *GIVEN* the source type is `ORACLE`
* *AND* an adapter emits a 4-way multi-statement IMPORT for source `SMOKE.ORACLE_BIG_T` (the Oracle adapter chose its own parallel split)
* *AND* the metadata cache reports `src_rows = 20000000` for `SMOKE.ORACLE_BIG_T`
* *AND* `OPTIONS` contains `PARALLEL_ROW_THRESHOLD=1000000;PARALLEL_STATEMENTS=AUTO;PARALLEL_SPLIT=AUTO`
* *WHEN* `MIGRATE_TO_EXASOL` is executed
* *THEN* the splitter MUST NOT modify the IMPORT
* *AND* all four original `statement '...'` clauses MUST be preserved in order
* *AND* the audit row for this IMPORT SHALL carry `SPLIT_STRATEGY = 'MULTI_PASSTHROUGH'`, `PARALLEL_EFFECTIVE = 4`

### Scenario: PARALLEL_SPLIT=OFF disables the splitter regardless of metadata

* *GIVEN* an adapter emits a single-statement IMPORT for `SMOKE.ORDERS` whose metadata has a numeric PK and would otherwise pick `PK_RANGE`
* *AND* `OPTIONS` contains `PARALLEL_ROW_THRESHOLD=1000000;PARALLEL_STATEMENTS=AUTO;PARALLEL_SPLIT=OFF`
* *WHEN* `MIGRATE_TO_EXASOL` is executed
* *THEN* the splitter MUST NOT modify the IMPORT
* *AND* the audit row SHALL carry `SPLIT_STRATEGY = 'SINGLE'`, `PARALLEL_EFFECTIVE = 1`

### Scenario: PARALLEL_SPLIT=DATE:CREATED_AT:QUARTER forces named col + grain

* *GIVEN* an adapter emits a single-statement IMPORT for `SMOKE.EVENTS`
* *AND* the metadata cache reports `src_rows = 20000000`, `src_pk_col = 'EVENT_ID'`, `src_date_col = 'EVENT_DT'`
* *AND* `OPTIONS` contains `PARALLEL_ROW_THRESHOLD=1000000;PARALLEL_STATEMENTS=4;PARALLEL_SPLIT=DATE:CREATED_AT:QUARTER`
* *WHEN* `MIGRATE_TO_EXASOL` is executed
* *THEN* the dispatcher SHALL rewrite the IMPORT into 4 `statement '...'` clauses keyed on `MONTH("CREATED_AT") IN (...)` quarters
* *AND* the splitter MUST NOT consult `src_pk_col` or `src_date_col` from the cache
* *AND* the audit row SHALL carry `SPLIT_STRATEGY = 'DATE_BUCKET'`, `SPLIT_KEY = 'CREATED_AT'`

### Scenario: PARALLEL_SPLIT=HASH:CUSTOMER_ID forces named col

* *GIVEN* an adapter emits a single-statement IMPORT for `SMOKE.SESSIONS`
* *AND* the metadata cache reports `src_rows = 20000000`, `src_pk_col = 'SESSION_ID'`
* *AND* `OPTIONS` contains `PARALLEL_ROW_THRESHOLD=1000000;PARALLEL_STATEMENTS=4;PARALLEL_SPLIT=HASH:CUSTOMER_ID`
* *WHEN* `MIGRATE_TO_EXASOL` is executed
* *THEN* the dispatcher SHALL rewrite the IMPORT into 4 `statement '...'` clauses each carrying `MOD(<dialect hash fn>("CUSTOMER_ID"), 4) = k`
* *AND* the splitter MUST NOT consult `src_pk_col` from the cache
* *AND* the audit row SHALL carry `SPLIT_STRATEGY = 'HASH_NUM'`, `SPLIT_KEY = 'CUSTOMER_ID'`

### Scenario: Forced override fails soft when prerequisite missing

* *GIVEN* an adapter emits a single-statement IMPORT for `SMOKE.HEAP_T`
* *AND* the source type is `SNOWFLAKE` (no ROWID support in v1)
* *AND* `OPTIONS` contains `PARALLEL_ROW_THRESHOLD=1000000;PARALLEL_STATEMENTS=4;PARALLEL_SPLIT=ROWID`
* *WHEN* `MIGRATE_TO_EXASOL` is executed
* *THEN* the splitter MUST NOT modify the IMPORT
* *AND* the dispatcher SHALL emit one `STEP_KIND = 'INFO'` row describing that `PARALLEL_SPLIT=ROWID` is unsupported for `SNOWFLAKE` and that the IMPORT fell back to `SINGLE`
* *AND* the audit row SHALL carry `SPLIT_STRATEGY = 'SINGLE'`, `PARALLEL_EFFECTIVE = 1`

### Scenario: Splitter rewrite preserves IMPORT target + column list

* *GIVEN* an adapter emits `IMPORT INTO "DST"."ORDERS" ("A","B","C") from JDBC at SRC statement 'select "A","B","C" from "PUBLIC"."orders"'` under `OPTIONS = 'TARGET_SCHEMA=DST;PARALLEL_ROW_THRESHOLD=1000000;PARALLEL_STATEMENTS=2;PARALLEL_SPLIT=AUTO'`
* *AND* the metadata cache reports `src_rows = 5000000`, `src_pk_col = 'A'`, `src_pk_type = 'NUMBER'`
* *WHEN* `MIGRATE_TO_EXASOL` is executed
* *THEN* the rewritten IMPORT MUST retain `IMPORT INTO "DST"."ORDERS" ("A","B","C")` byte-for-byte
* *AND* each emitted `statement '...'` MUST quote the same column list (`"A","B","C"`) in the same order
* *AND* each emitted inner SELECT MUST reference `from "PUBLIC"."orders"` (the source identifier, not the target)
* *AND* the audit row SHALL carry `SPLIT_STRATEGY = 'PK_RANGE'`, `SPLIT_KEY = 'A'`, `PARALLEL_EFFECTIVE = 2`

### Scenario: Rewrite failure leaves IMPORT unchanged and continues migration

* *GIVEN* an adapter emits a single-statement IMPORT for `SMOKE.WEIRD_T`
* *AND* the metadata cache reports `src_rows = 20000000`, `src_pk_col = 'ID'`, `src_pk_type = 'NUMBER'`
* *AND* the inner SELECT contains a shape `pick_split_strategy` cannot parse (e.g. nested subquery the helper does not recognize)
* *AND* `OPTIONS` contains `PARALLEL_ROW_THRESHOLD=1000000;PARALLEL_STATEMENTS=4;PARALLEL_SPLIT=AUTO`
* *WHEN* `MIGRATE_TO_EXASOL` is executed
* *THEN* the dispatcher MUST NOT raise the migration as failed solely because of the rewrite failure
* *AND* the IMPORT row SHALL be passed through unchanged
* *AND* the dispatcher SHALL emit one `STEP_KIND = 'INFO'` row describing the soft-fail and the table involved
* *AND* the audit row for this IMPORT SHALL carry `SPLIT_STRATEGY = 'SINGLE'`, `PARALLEL_EFFECTIVE = 1`
