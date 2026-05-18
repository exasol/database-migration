# Feature: source-metadata-roundtrip

The dispatcher (`MIGRATE_TO_EXASOL`) batches every per-table fact it needs from the source database into a single `IMPORT FROM JDBC` query per migration. The fetched cache is keyed by source `(schema, table)` and consumed by `transform_for_gate` (Speq 1) and `transform_for_split` (this speq). The round-trip is dispatched off a per-source SQL table (`SOURCE_METADATA_BY_SOURCE`) that supersedes Speq 1's narrower `ROW_COUNT_SQL_BY_SOURCE`. The round-trip is soft-failing: on any error the cache is populated with all-NULL rows, an `INFO` row is emitted, and the migration continues with single-statement IMPORTs.

## Background

* All scenarios use `MIGRATE_TO_EXASOL` as the entry point.
* The cache schema is one row per `(src_schema, src_table)` pair containing:
  * `src_schema   VARCHAR`
  * `src_table    VARCHAR`
  * `src_rows     DECIMAL(36,0)`
  * `src_pk_col   VARCHAR`
  * `src_pk_type  VARCHAR`
  * `src_date_col VARCHAR`
  * `src_num_col  VARCHAR`
  * `src_partitioned BOOLEAN`
* `SOURCE_METADATA_BY_SOURCE` is a Lua dispatch table inside `migrate_to_exasol.sql` keyed by `SOURCE_TYPE` (e.g. `POSTGRES`, `MYSQL`, `SQLSERVER`, `AZURE_SQL`, `SNOWFLAKE`, `BIGQUERY`, `REDSHIFT`, `VERTICA`, `DB2`, `HANA`, `NETEZZA`, `TERADATA`, `DATABRICKS`, `ORACLE`).
* Each entry is a SQL template returning the 8-column schema above. The dispatcher binds the requested `(schema, table)` pairs as an `OR`-of-equality predicate inside one `IMPORT FROM JDBC at <CONN> statement '<SQL>'`.
* Sources that cannot supply a given column (e.g. Databricks cannot supply `src_rows`) MUST return `NULL` for that column; the dispatcher MUST NOT treat the row as a fetch failure.
* `transform_for_metadata` runs before `transform_for_gate` and `transform_for_split`. The gate (Speq 1) reads `src_rows` from this cache instead of issuing its own row-count query; the splitter reads the remaining columns.
* `PARALLEL_ROW_THRESHOLD=0` (Speq 1) disables the gate; it does NOT disable the metadata round-trip — the splitter still needs the cache. `PARALLEL_STATEMENTS=1` AND `PARALLEL_SPLIT=OFF` together disable both consumers; in that case `transform_for_metadata` MUST be skipped to spare the source-side query.
* Source-side `(schema, table)` references are parsed out of each IMPORT's inner SELECT (`from "<schema>"."<table>"`), reusing the helper introduced in Speq 1. Target-side identifiers (`TARGET_SCHEMA`, `IDENTIFIER_CASE_INSENSITIVE`, `CATALOG2SCHEMA`) MUST NOT influence the cache key.

## Scenarios

### Scenario: One round-trip per migration regardless of table count

* *GIVEN* an adapter emits five IMPORTs covering five distinct source tables in one run
* *AND* `OPTIONS` contains `PARALLEL_ROW_THRESHOLD=1000000;PARALLEL_STATEMENTS=AUTO`
* *WHEN* `MIGRATE_TO_EXASOL` is executed
* *THEN* the dispatcher SHALL issue exactly one `IMPORT FROM JDBC at <CONN> statement '...'` against the source for metadata gathering
* *AND* the single query's WHERE clause MUST enumerate all five `(schema, table)` pairs (e.g. via `OR`-of-equality on `(schema_name, table_name)`)

### Scenario: Cache is consumed by both gate and splitter

* *GIVEN* an adapter emits one multi-statement IMPORT for `SMOKE.SMALL_T` and one single-statement IMPORT for `SMOKE.BIG_T`
* *AND* the metadata round-trip returns `src_rows = 500` for `SMOKE.SMALL_T` and `src_rows = 20000000, src_pk_col = 'ID', src_pk_type = 'NUMBER'` for `SMOKE.BIG_T`
* *AND* `OPTIONS` contains `PARALLEL_ROW_THRESHOLD=1000000;PARALLEL_STATEMENTS=4;PARALLEL_SPLIT=AUTO`
* *WHEN* `MIGRATE_TO_EXASOL` is executed
* *THEN* `transform_for_gate` SHALL collapse `SMOKE.SMALL_T`'s IMPORT to a single statement using `src_rows = 500` from the cache
* *AND* `transform_for_split` SHALL rewrite `SMOKE.BIG_T`'s IMPORT into 4 `statement '...'` clauses using `src_pk_col = 'ID'` from the same cache
* *AND* the dispatcher MUST NOT issue a second source-side metadata query

### Scenario: Per-source SQL dispatched from SOURCE_METADATA_BY_SOURCE

* *GIVEN* the source type is `POSTGRES`
* *AND* an adapter emits at least one IMPORT for `PUBLIC.ORDERS`
* *WHEN* `MIGRATE_TO_EXASOL` is executed
* *THEN* the metadata round-trip SHALL use the `POSTGRES` entry of `SOURCE_METADATA_BY_SOURCE`
* *AND* the per-source SQL SHALL join `pg_class`, `pg_namespace`, `pg_constraint`, and `pg_attribute` to populate all eight cache columns
* *AND* swapping `SOURCE_TYPE` to `MYSQL` for the same fixture SHALL instead dispatch the `MYSQL` entry (joining `information_schema.tables`, `key_column_usage`, `columns`) without any other code path change

### Scenario: Sources without per-column support return NULL not error

* *GIVEN* the source type is `DATABRICKS`
* *AND* an adapter emits one IMPORT for `MAIN.DEFAULT.EVENTS`
* *AND* the `DATABRICKS` entry of `SOURCE_METADATA_BY_SOURCE` does not expose a per-table row count
* *WHEN* `MIGRATE_TO_EXASOL` is executed
* *THEN* the metadata round-trip MUST complete without raising
* *AND* the cache entry for `MAIN.DEFAULT.EVENTS` SHALL carry `src_rows = NULL`
* *AND* downstream `transform_for_gate` MUST treat NULL `src_rows` per Speq 1 semantics
* *AND* downstream `transform_for_split` MUST skip the splitter for the row (NULL row count → cannot decide threshold eligibility)

### Scenario: Round-trip failure populates all-NULL cache and emits INFO row

* *GIVEN* an adapter emits at least one IMPORT
* *AND* the metadata round-trip fails (network error, missing privilege, or unsupported metadata view)
* *WHEN* `MIGRATE_TO_EXASOL` is executed
* *THEN* the dispatcher MUST NOT raise the migration as failed solely because of the round-trip failure
* *AND* the dispatcher SHALL populate the cache with all-NULL rows for every requested `(schema, table)` pair
* *AND* the dispatcher SHALL emit one `STEP_KIND = 'INFO'` row describing that source-side metadata fetch was skipped
* *AND* `transform_for_gate` SHALL pass every IMPORT through unchanged
* *AND* `transform_for_split` SHALL pass every IMPORT through unchanged

### Scenario: Source type with no SOURCE_METADATA_BY_SOURCE entry skips fetch

* *GIVEN* `SOURCE_TYPE` resolves to an adapter for which the dispatcher does not ship a metadata SQL (e.g. `EXASOL`, `VECTORWISE`, `S3`)
* *AND* `OPTIONS` contains `PARALLEL_ROW_THRESHOLD=1000000;PARALLEL_STATEMENTS=AUTO`
* *WHEN* `MIGRATE_TO_EXASOL` is executed
* *THEN* the dispatcher MUST NOT issue any source-side metadata query
* *AND* the dispatcher SHALL emit one `STEP_KIND = 'INFO'` row noting that the metadata cache is not configured for this source type
* *AND* every emitted IMPORT SHALL pass through `transform_for_gate` and `transform_for_split` unchanged

### Scenario: Adapter emits no IMPORTs at all skips the round-trip

* *GIVEN* an adapter returns only DDL rows (no `IMPORT INTO ...` statements)
* *AND* `OPTIONS` contains a non-zero `PARALLEL_ROW_THRESHOLD`
* *WHEN* `MIGRATE_TO_EXASOL` is executed
* *THEN* the dispatcher MUST NOT issue any source-side metadata query
* *AND* the dispatcher SHALL pass adapter output through to `normalize_rows` / `execute_generated_sql` unchanged

### Scenario: Cache key uses source identifier not target rename

* *GIVEN* an adapter emits `IMPORT INTO "DST"."ORDERS" (...) from JDBC at SRC statement 'select ... from "PUBLIC"."orders"'` under `OPTIONS = 'TARGET_SCHEMA=DST;PARALLEL_ROW_THRESHOLD=1000000'`
* *WHEN* `MIGRATE_TO_EXASOL` is executed
* *THEN* the metadata round-trip's WHERE clause SHALL key on `(PUBLIC, orders)` (the source identifier parsed from the inner SELECT's `from "..."."..."` clause)
* *AND* the target-side identifier `(DST, ORDERS)` MUST NOT appear in the metadata round-trip's WHERE clause

### Scenario: Duplicate source tables in adapter output produce one cache row

* *GIVEN* an adapter emits two IMPORTs both targeting source `PUBLIC.orders` (e.g. one full-table, one filtered subset)
* *WHEN* `MIGRATE_TO_EXASOL` is executed
* *THEN* the metadata round-trip's WHERE clause MUST contain `(PUBLIC, orders)` exactly once
* *AND* the cache MUST yield a single row keyed on `(PUBLIC, orders)`
* *AND* both downstream IMPORTs MUST resolve to the same cache entry

### Scenario: Per-source SQL is a pure dispatch lookup, not a code branch

* *GIVEN* a new source type `<NEW_SRC>` is added to `SOURCE_METADATA_BY_SOURCE` with a SQL template returning the 8-column schema
* *AND* no other change is made to `migrate_to_exasol.sql`
* *AND* no `<new_src>_to_exasol.sql` adapter file is modified
* *WHEN* `MIGRATE_TO_EXASOL` is executed against the new source
* *THEN* `transform_for_metadata` SHALL dispatch the new SQL successfully via the table lookup
* *AND* `transform_for_gate` and `transform_for_split` SHALL consume the resulting cache without any source-specific branching

### Scenario: Splitter + gate both disabled skips the round-trip

* *GIVEN* an adapter emits at least one IMPORT
* *AND* `OPTIONS` contains `PARALLEL_ROW_THRESHOLD=0;PARALLEL_STATEMENTS=1;PARALLEL_SPLIT=OFF`
* *WHEN* `MIGRATE_TO_EXASOL` is executed
* *THEN* the dispatcher MUST NOT issue any source-side metadata query
* *AND* every emitted IMPORT SHALL pass through unchanged
* *AND* the audit rows SHALL carry `SPLIT_STRATEGY = 'SINGLE'` and `PARALLEL_EFFECTIVE = 1` for every IMPORT
