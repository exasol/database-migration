# Feature: parallel-auto-ceiling

The dispatcher (`MIGRATE_TO_EXASOL`) resolves the `PARALLEL_STATEMENTS` OPTIONS value into a per-table effective integer `N` consumed by `transform_for_split`. `AUTO` (the default) maps to a row-count heuristic capped by a separate `PARALLEL_AUTO_CEILING` OPTIONS knob (default 12). Explicit integer values bypass the cap. Resolution is purely a function of OPTIONS + cache; no source-side query is involved at resolution time. The resolved value is recorded in `PARALLEL_EFFECTIVE` (see `split-audit-columns`).

## Background

* All scenarios use `MIGRATE_TO_EXASOL` as the entry point.
* `PARALLEL_STATEMENTS` accepts `AUTO` (default), or a positive integer `N ≥ 1`. Negative / zero / non-numeric / non-AUTO values are invalid and MUST raise.
* `PARALLEL_AUTO_CEILING` accepts a positive integer (default `12`). Negative / zero / non-numeric values are invalid and MUST raise.
* `AUTO` heuristic per table: `effective_n = min(ceiling, max(1, ceil(src_rows / 5_000_000)))`.
* `AUTO` with NULL or unknown `src_rows` → `effective_n = 1` (the splitter cannot decide threshold eligibility anyway; gate-collapse semantics from Speq 1 apply).
* `AUTO` with `src_rows` below `PARALLEL_ROW_THRESHOLD` → `effective_n = 1` (threshold gate decides; this is reported in the audit row but the splitter does not fire).
* Explicit integer N bypasses the ceiling. Users who set `PARALLEL_STATEMENTS=24` are stating they know their source connection pool.
* Resolution is per-IMPORT, not per-migration: two IMPORTs in the same migration with different row counts can resolve to different `effective_n` under `AUTO`.

## Scenarios

### Scenario: AUTO with src_rows below threshold resolves to 1

* *GIVEN* the metadata cache reports `src_rows = 500000` for an IMPORT
* *AND* `OPTIONS` contains `PARALLEL_ROW_THRESHOLD=1000000;PARALLEL_STATEMENTS=AUTO`
* *WHEN* `MIGRATE_TO_EXASOL` is executed
* *THEN* `resolve_parallel_statements` SHALL return `effective_n = 1` for this IMPORT
* *AND* the audit row SHALL carry `PARALLEL_REQUESTED = 'AUTO'`, `PARALLEL_EFFECTIVE = 1`

### Scenario: AUTO with mid-range row count resolves via heuristic

* *GIVEN* the metadata cache reports `src_rows = 20000000` for an IMPORT
* *AND* `OPTIONS` contains `PARALLEL_ROW_THRESHOLD=1000000;PARALLEL_STATEMENTS=AUTO`
* *WHEN* `MIGRATE_TO_EXASOL` is executed
* *THEN* `resolve_parallel_statements` SHALL return `effective_n = 4` (ceil(20M / 5M) = 4, under the default cap 12)
* *AND* the audit row SHALL carry `PARALLEL_REQUESTED = 'AUTO'`, `PARALLEL_EFFECTIVE = 4`

### Scenario: AUTO with row count exceeding default ceiling caps at 12

* *GIVEN* the metadata cache reports `src_rows = 100000000` for an IMPORT
* *AND* `OPTIONS` contains `PARALLEL_ROW_THRESHOLD=1000000;PARALLEL_STATEMENTS=AUTO`
* *WHEN* `MIGRATE_TO_EXASOL` is executed
* *THEN* `resolve_parallel_statements` SHALL return `effective_n = 12` (ceil(100M / 5M) = 20, capped at default ceiling 12)
* *AND* the audit row SHALL carry `PARALLEL_REQUESTED = 'AUTO'`, `PARALLEL_EFFECTIVE = 12`

### Scenario: PARALLEL_AUTO_CEILING raises the AUTO cap

* *GIVEN* the metadata cache reports `src_rows = 100000000` for an IMPORT
* *AND* `OPTIONS` contains `PARALLEL_ROW_THRESHOLD=1000000;PARALLEL_STATEMENTS=AUTO;PARALLEL_AUTO_CEILING=24`
* *WHEN* `MIGRATE_TO_EXASOL` is executed
* *THEN* `resolve_parallel_statements` SHALL return `effective_n = 20` (ceil(100M / 5M) = 20, under the raised cap 24)
* *AND* the audit row SHALL carry `PARALLEL_REQUESTED = 'AUTO'`, `PARALLEL_EFFECTIVE = 20`

### Scenario: Explicit PARALLEL_STATEMENTS bypasses the ceiling

* *GIVEN* the metadata cache reports `src_rows = 100000000` for an IMPORT
* *AND* `OPTIONS` contains `PARALLEL_ROW_THRESHOLD=1000000;PARALLEL_STATEMENTS=24`
* *WHEN* `MIGRATE_TO_EXASOL` is executed
* *THEN* `resolve_parallel_statements` SHALL return `effective_n = 24` (ignoring both the default ceiling 12 and any `PARALLEL_AUTO_CEILING` value)
* *AND* the audit row SHALL carry `PARALLEL_REQUESTED = '24'`, `PARALLEL_EFFECTIVE = 24`

### Scenario: AUTO with NULL row count resolves to 1

* *GIVEN* the metadata cache reports `src_rows = NULL` for an IMPORT (e.g. Databricks, or post-soft-fail all-NULL cache)
* *AND* `OPTIONS` contains `PARALLEL_ROW_THRESHOLD=1000000;PARALLEL_STATEMENTS=AUTO`
* *WHEN* `MIGRATE_TO_EXASOL` is executed
* *THEN* `resolve_parallel_statements` SHALL return `effective_n = 1`
* *AND* the audit row SHALL carry `PARALLEL_REQUESTED = 'AUTO'`, `PARALLEL_EFFECTIVE = 1`
* *AND* the splitter MUST NOT fire on this IMPORT regardless of metadata for other columns

### Scenario: Per-IMPORT resolution within one migration

* *GIVEN* an adapter emits three IMPORTs in one run
* *AND* the metadata cache reports `src_rows = 500000` for `SMOKE.SMALL_T`, `src_rows = 20000000` for `SMOKE.BIG_T`, and `src_rows = 100000000` for `SMOKE.HUGE_T`
* *AND* `OPTIONS` contains `PARALLEL_ROW_THRESHOLD=1000000;PARALLEL_STATEMENTS=AUTO`
* *WHEN* `MIGRATE_TO_EXASOL` is executed
* *THEN* the dispatcher SHALL resolve `effective_n = 1` for `SMOKE.SMALL_T`, `effective_n = 4` for `SMOKE.BIG_T`, and `effective_n = 12` for `SMOKE.HUGE_T`
* *AND* the audit rows SHALL carry the three different `PARALLEL_EFFECTIVE` values
* *AND* `PARALLEL_REQUESTED` SHALL be `'AUTO'` for all three rows

### Scenario: Invalid PARALLEL_STATEMENTS value raises

* *GIVEN* `OPTIONS` contains `PARALLEL_STATEMENTS=0`, `PARALLEL_STATEMENTS=-3`, or `PARALLEL_STATEMENTS=banana`
* *WHEN* `MIGRATE_TO_EXASOL` is executed
* *THEN* the dispatcher MUST raise an error referencing the offending OPTIONS key and value
* *AND* the dispatcher MUST NOT execute any adapter SQL nor any source-side query

### Scenario: Invalid PARALLEL_AUTO_CEILING value raises

* *GIVEN* `OPTIONS` contains `PARALLEL_AUTO_CEILING=0`, `PARALLEL_AUTO_CEILING=-1`, or `PARALLEL_AUTO_CEILING=many`
* *WHEN* `MIGRATE_TO_EXASOL` is executed
* *THEN* the dispatcher MUST raise an error referencing the offending OPTIONS key and value
* *AND* the dispatcher MUST NOT execute any adapter SQL nor any source-side query
