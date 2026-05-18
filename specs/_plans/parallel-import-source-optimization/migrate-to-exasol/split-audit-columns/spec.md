# Feature: split-audit-columns

The dispatcher (`MIGRATE_TO_EXASOL`) appends four new audit columns to its output row schema so operators can post-hoc analyze how the gate (Speq 1) and splitter (this speq) decided to handle each IMPORT. `OUT_COLUMNS` grows from 7 columns (introduced in Speq 1) to 11 columns. Non-IMPORT rows (DDL, INFO, SUMMARY) carry NULL in all four new columns. The bump is a documented breaking change for any caller that parses the master-script output positionally; direct adapter callers (`<SRC>_TO_EXASOL`) are unaffected because adapters do not emit the audit columns at all.

## Background

* All scenarios use `MIGRATE_TO_EXASOL` as the entry point.
* The four new columns, appended after the existing 7 Speq-1 columns:
  * `SPLIT_STRATEGY VARCHAR(32)` — one of `PARTITION`, `PK_RANGE`, `UNIQUE_NUM`, `DATE_BUCKET`, `HASH_NUM`, `ROWID`, `SINGLE`, `MULTI_PASSTHROUGH`, or `NULL` for non-IMPORT rows
  * `SPLIT_KEY VARCHAR(256)` — column / pseudo-column / partition name actually used as the parallel selector; `NULL` for `SINGLE`, `MULTI_PASSTHROUGH`, and non-IMPORT rows
  * `PARALLEL_REQUESTED VARCHAR(16)` — raw `PARALLEL_STATEMENTS` OPTIONS value (`AUTO`, `8`, etc.); `NULL` for non-IMPORT rows
  * `PARALLEL_EFFECTIVE DECIMAL(4,0)` — actual count of `statement '...'` clauses in the emitted IMPORT; `NULL` for non-IMPORT rows
* The audit columns are populated by `transform_for_audit`, which runs after `transform_for_metadata` → `transform_for_gate` → `transform_for_split` so it sees the final decision for every row.
* `MULTI_PASSTHROUGH` covers Oracle's pre-existing multi-statement IMPORTs that the splitter intentionally skips.
* `SINGLE` covers: (a) gate-collapsed multi-stmt IMPORTs, (b) below-threshold pass-throughs, (c) splitter fallbacks (no usable strategy, soft-fail), (d) `PARALLEL_SPLIT=OFF`.
* The bump from 7 → 11 columns is announced in CHANGELOG; the `OUT_COLUMNS` declaration in `migrate_to_exasol.sql` is the canonical source.
* `transform_for_gate`'s INFO-row constructor (Speq 1) and any other path that builds rows MUST be updated to emit 11-tuple rows with NULLs in the four new columns.

## Scenarios

### Scenario: IMPORT after splitter PK_RANGE rewrite gets full audit row

* *GIVEN* an adapter emits a single-statement IMPORT for `SMOKE.ORDERS`
* *AND* `transform_for_split` rewrites it into 4 `statement '...'` clauses using PK_RANGE on `ORDER_ID`
* *AND* `OPTIONS` contains `PARALLEL_STATEMENTS=AUTO`
* *WHEN* the dispatcher emits its output rows
* *THEN* the IMPORT row's audit columns SHALL be: `SPLIT_STRATEGY = 'PK_RANGE'`, `SPLIT_KEY = 'ORDER_ID'`, `PARALLEL_REQUESTED = 'AUTO'`, `PARALLEL_EFFECTIVE = 4`
* *AND* the row SHALL be an 11-tuple in `OUT_COLUMNS` order

### Scenario: Gate-collapsed IMPORT records SINGLE strategy

* *GIVEN* an adapter emits a 4-way multi-statement IMPORT for `SMOKE.SMALL_T`
* *AND* the metadata cache reports `src_rows = 500` for `SMOKE.SMALL_T`
* *AND* `OPTIONS` contains `PARALLEL_ROW_THRESHOLD=1000000;PARALLEL_STATEMENTS=AUTO`
* *AND* `transform_for_gate` collapses the IMPORT to a single statement
* *WHEN* the dispatcher emits its output rows
* *THEN* the IMPORT row's audit columns SHALL be: `SPLIT_STRATEGY = 'SINGLE'`, `SPLIT_KEY = NULL`, `PARALLEL_REQUESTED = 'AUTO'`, `PARALLEL_EFFECTIVE = 1`

### Scenario: Oracle multi-statement passthrough records MULTI_PASSTHROUGH

* *GIVEN* the source type is `ORACLE`
* *AND* the adapter emits a 4-way multi-statement IMPORT for `SMOKE.ORACLE_BIG_T` whose metadata reports `src_rows = 20000000`
* *AND* `OPTIONS` contains `PARALLEL_ROW_THRESHOLD=1000000;PARALLEL_STATEMENTS=AUTO`
* *WHEN* the dispatcher emits its output rows
* *THEN* the IMPORT row's audit columns SHALL be: `SPLIT_STRATEGY = 'MULTI_PASSTHROUGH'`, `SPLIT_KEY = NULL`, `PARALLEL_REQUESTED = 'AUTO'`, `PARALLEL_EFFECTIVE = 4`

### Scenario: DDL rows carry NULL audit columns

* *GIVEN* an adapter emits a `CREATE SCHEMA`, a `CREATE TABLE`, and one IMPORT in one run
* *WHEN* the dispatcher emits its output rows
* *THEN* the `CREATE SCHEMA` row and the `CREATE TABLE` row SHALL each carry `SPLIT_STRATEGY = NULL`, `SPLIT_KEY = NULL`, `PARALLEL_REQUESTED = NULL`, `PARALLEL_EFFECTIVE = NULL`
* *AND* only the IMPORT row SHALL carry non-NULL audit columns

### Scenario: INFO rows carry NULL audit columns

* *GIVEN* a migration triggers any `STEP_KIND = 'INFO'` row (e.g. metadata round-trip soft-fail, splitter fallback, source type without metadata SQL)
* *WHEN* the dispatcher emits its output rows
* *THEN* every `INFO` row SHALL carry `SPLIT_STRATEGY = NULL`, `SPLIT_KEY = NULL`, `PARALLEL_REQUESTED = NULL`, `PARALLEL_EFFECTIVE = NULL`

### Scenario: SUMMARY row carries NULL audit columns

* *GIVEN* a migration emits its final `STEP_KIND = 'SUMMARY'` row (introduced in Speq 1)
* *WHEN* the dispatcher emits its output rows
* *THEN* the `SUMMARY` row SHALL carry `SPLIT_STRATEGY = NULL`, `SPLIT_KEY = NULL`, `PARALLEL_REQUESTED = NULL`, `PARALLEL_EFFECTIVE = NULL`

### Scenario: Per-IMPORT audit independence

* *GIVEN* an adapter emits three IMPORTs whose final fates differ: `SMOKE.SMALL_T` gate-collapsed to single, `SMOKE.BIG_T` splitter-rewritten via PK_RANGE on `ID` into 4 clauses, `SMOKE.DATED_T` splitter-rewritten via DATE_BUCKET on `EVENT_DT` into 4 clauses
* *AND* `OPTIONS` contains `PARALLEL_ROW_THRESHOLD=1000000;PARALLEL_STATEMENTS=4`
* *WHEN* the dispatcher emits its output rows
* *THEN* the `SMOKE.SMALL_T` row SHALL carry `SPLIT_STRATEGY = 'SINGLE'`, `SPLIT_KEY = NULL`, `PARALLEL_REQUESTED = '4'`, `PARALLEL_EFFECTIVE = 1`
* *AND* the `SMOKE.BIG_T` row SHALL carry `SPLIT_STRATEGY = 'PK_RANGE'`, `SPLIT_KEY = 'ID'`, `PARALLEL_REQUESTED = '4'`, `PARALLEL_EFFECTIVE = 4`
* *AND* the `SMOKE.DATED_T` row SHALL carry `SPLIT_STRATEGY = 'DATE_BUCKET'`, `SPLIT_KEY = 'EVENT_DT'`, `PARALLEL_REQUESTED = '4'`, `PARALLEL_EFFECTIVE = 4`

### Scenario: OUT_COLUMNS schema bumped to 11

* *GIVEN* `migrate_to_exasol.sql` after this speq lands
* *WHEN* a caller invokes `MIGRATE_TO_EXASOL` with `DEBUG=TRUE` (returns rows instead of executing them)
* *THEN* each returned row MUST be an 11-tuple matching `OUT_COLUMNS` in declared order
* *AND* the column names in declared order SHALL end with `SPLIT_STRATEGY`, `SPLIT_KEY`, `PARALLEL_REQUESTED`, `PARALLEL_EFFECTIVE`

### Scenario: PARALLEL_REQUESTED records the raw OPTIONS value

* *GIVEN* `OPTIONS` contains `PARALLEL_STATEMENTS=8`
* *AND* an adapter emits one IMPORT for which `transform_for_split` produces 8 `statement '...'` clauses
* *WHEN* the dispatcher emits its output rows
* *THEN* the IMPORT row's `PARALLEL_REQUESTED` SHALL be the literal string `'8'`
* *AND* the IMPORT row's `PARALLEL_EFFECTIVE` SHALL be `8`

### Scenario: SPLIT_KEY echoes the pseudo-column for ROWID strategy

* *GIVEN* the source type is `POSTGRES`
* *AND* an adapter emits a single-statement IMPORT for which `transform_for_split` falls through to step 6 (ROWID)
* *AND* `OPTIONS` contains `PARALLEL_STATEMENTS=4`
* *WHEN* the dispatcher emits its output rows
* *THEN* the IMPORT row's audit columns SHALL be: `SPLIT_STRATEGY = 'ROWID'`, `SPLIT_KEY = 'ctid'`, `PARALLEL_REQUESTED = '4'`, `PARALLEL_EFFECTIVE = 4`
