# Mission: database-migration

> Provide Exasol SQL scripts that generate schema, table, and import statements for moving data from external database systems into Exasol.

## Problem Statement

Exasol users need repeatable migration helpers that inspect source database metadata and generate the Exasol DDL and `IMPORT` statements needed to load data. Manually writing these statements for every schema, table, column, and data type is slow and error-prone, especially across heterogeneous database systems.

## Target Users

| Persona | Goal | Key Workflow |
|---------|------|--------------|
| Exasol administrator | Load data from an external database into Exasol | Configure a connection, create the migration script, execute it to generate SQL, then run the generated SQL |
| Data engineer | Adapt migration behavior for a source system | Review a source-specific script, adjust type mappings or filters, then test generated SQL against Exasol |
| Community contributor | Add or improve source support | Follow the existing source-script pattern, document setup, and add regression coverage |

## Core Capabilities

1. **Source metadata discovery** — Query external database metadata through Exasol connections.
2. **Migration SQL generation** — Generate ordered `CREATE SCHEMA`, `CREATE TABLE`, and `IMPORT` statements.
3. **Source-specific type mapping** — Convert source data types into Exasol-compatible column definitions.
4. **Post-load helpers** — Provide optional optimization scripts after data has loaded.
5. **Delta import helpers** — Provide scripts for timestamp-based incremental imports.

## Out of Scope

- Managed migration service orchestration.
- Credential lifecycle management.
- Guaranteed official support; the README states this is not an officially supported Exasol product.
- Replacing source-specific loader scripts with a single universal interface in all cases.
- S3-specific loading through the generic JDBC adapter pattern.

## Domain Glossary

| Term | Definition |
|------|------------|
| Exasol script | A SQL-created Lua script executed inside Exasol with `EXECUTE SCRIPT`. |
| Migration source | The external system from which data is loaded, such as MySQL, Snowflake, or Databricks. |
| Connection | An Exasol connection object used by `IMPORT FROM JDBC` or another import method. |
| Generated SQL | SQL rows returned by migration scripts for later execution. |
| Identifier case insensitive | Option that uppercases generated Exasol identifiers for case-insensitive handling. |
| Source filter | Pattern restricting which catalogs, schemas, or tables are included. |

---

## Tech Stack

| Layer | Technology | Purpose |
|-------|------------|---------|
| Database runtime | Exasol SQL and Lua scripts | Generate migration SQL from inside Exasol |
| Connectivity | Exasol `IMPORT FROM JDBC` and connection objects | Query source metadata and import source rows |
| Test utilities | Python with EXAODBC, shell scripts, Docker | Existing integration-style test harness |
| Lightweight tests | Lua | Mock Exasol script execution for source-script regression tests |

## Commands

```bash
# Build
python3 -c "import ast, pathlib; [ast.parse(pathlib.Path(p).read_text(), filename=p) for p in ('test/create_script.py','test/export_res.py','test/mock_test.py')]"

# Test
lua test/test_databricks_to_exasol.lua

# Lint & Format
git diff --check

# Gather Code Coverage
# No coverage command is defined for this repository.
```

## Project Structure

```
database-migration/
├── *_to_exasol.sql              # Source-specific migration scripts
├── s3_to_exasol.sql             # S3 parallel loader with separate parameter shape
├── post_load_optimization/      # Optional post-load optimization scripts
├── delta_import/                # Delta import helper scripts
├── test/                        # Existing integration harness and source fixtures
└── specs/                       # Speq mission and active plans
```

## Architecture

The repository uses a source-specific script pattern. Each migration script creates a `database_migration.<SOURCE>_TO_EXASOL` Exasol Lua script, queries source metadata through an Exasol connection, transforms metadata with Exasol SQL, and returns ordered SQL rows. Users review or execute the generated DDL and `IMPORT` statements in a separate step.

### Master-script architecture (durable invariant)

`migrate_to_exasol.sql` is the orchestrator on top of the per-source adapter scripts. It is the **only** place cross-cutting behavior is allowed to grow. Adapter scripts (`<source>_to_exasol.sql`) are frozen at their current shape and are never touched by orchestrator features — they remain authoritative for source-specific schema discovery, type mapping, identifier quoting, DDL generation, and `IMPORT` shape, so the SE team and existing customers who call adapters directly continue to work byte-for-byte.

The orchestrator implements cross-cutting concerns as a pipeline of post-processing transforms over the rows the adapter returned:

1. `transform_for_metadata` — one source-side `IMPORT FROM JDBC` per migration that fetches per-(schema, table) row counts plus PK / date / numeric / partition hints. Backed by a `SOURCE_METADATA_BY_SOURCE` dispatch table. Soft-fails to an all-NULL cache + `INFO` row.
2. `transform_for_gate` — collapses multi-statement IMPORTs whose source row count is below `PARALLEL_ROW_THRESHOLD` to a single statement.
3. `transform_for_split` — expands single-statement IMPORTs at-or-above the threshold into N parallel `STATEMENT '...'` clauses with pushdown-friendly WHERE selectors. Walks a per-source split-strategy hierarchy backed by a `DIALECT_BY_SOURCE` dispatch table.
4. `transform_for_audit` — records the gate/splitter decision per row into four audit columns (`SPLIT_STRATEGY`, `SPLIT_KEY`, `PARALLEL_REQUESTED`, `PARALLEL_EFFECTIVE`).

Any transform that fails (lookup error, unparseable IMPORT, missing dialect entry) must **soft-fail**: emit an `INFO` row describing the skip and leave the affected adapter rows unchanged. The orchestrator's transforms are optimizations and must never be allowed to break a migration that the adapter alone would have completed.

Adding a new source means adding entries to `SOURCE_METADATA_BY_SOURCE` and `DIALECT_BY_SOURCE` (and the dispatch branch in `MIGRATE_TO_EXASOL` itself) — never modifying the adapter file.

## Constraints

- **Technical**: Scripts must run inside Exasol and use Exasol-supported Lua and SQL syntax.
- **Technical**: Source access depends on externally configured Exasol connection objects and available JDBC drivers.
- **Business**: The project is open source and not officially supported by Exasol.
- **Performance**: Scripts should generate SQL from metadata without loading source data until generated `IMPORT` statements run.

## External Dependencies

| Service | Purpose | Failure Impact |
|---------|---------|----------------|
| Source database | Provides metadata and row data for migration | Migration script cannot discover or load source tables |
| Exasol connection object | Stores endpoint and credential details | `IMPORT FROM JDBC` fails |
| JDBC driver | Enables Exasol to connect to a source system | Metadata and data imports fail |
