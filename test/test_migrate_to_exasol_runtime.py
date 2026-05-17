#!/usr/bin/env python3
"""Runtime smoke test for MIGRATE_TO_EXASOL using local mock adapters.

This test replaces scripts in DATABASE_MIGRATION on the target Exasol database.
Run it only against a disposable local database.
"""

from __future__ import annotations

import os
import re
import ssl
import time
from pathlib import Path

import pyexasol


REPO = Path(__file__).resolve().parents[1]

ADAPTERS = {
    "MYSQL": ("MYSQL_TO_EXASOL", "CONNECTION_NAME, IDENTIFIER_CASE_INSENSITIVE, SCHEMA_FILTER, TABLE_FILTER"),
    "MARIADB": ("MARIADB_TO_EXASOL", "CONNECTION_NAME, IDENTIFIER_CASE_INSENSITIVE, SCHEMA_FILTER, TABLE_FILTER"),
    "POSTGRES": ("POSTGRES_TO_EXASOL", "CONNECTION_NAME, IDENTIFIER_CASE_INSENSITIVE, SCHEMA_FILTER, TABLE_FILTER, TARGET_SCHEMA"),
    "REDSHIFT": ("REDSHIFT_TO_EXASOL", "CONNECTION_NAME, IDENTIFIER_CASE_INSENSITIVE, SCHEMA_FILTER, TABLE_FILTER"),
    "DB2": ("DB2_TO_EXASOL", "CONNECTION_NAME, IDENTIFIER_CASE_INSENSITIVE, SCHEMA_FILTER, TABLE_FILTER"),
    "VERTICA": ("VERTICA_TO_EXASOL", "CONNECTION_NAME, IDENTIFIER_CASE_INSENSITIVE, SCHEMA_FILTER, TABLE_FILTER"),
    "HANA": ("HANA_TO_EXASOL", "CONNECTION_NAME, IDENTIFIER_CASE_INSENSITIVE, SCHEMA_FILTER, TABLE_FILTER"),
    "AZURE_SQL": ("AZURE_SQL_TO_EXASOL", "CONNECTION_NAME, SCHEMA_FILTER, TABLE_FILTER, IDENTIFIER_CASE_INSENSITIVE"),
    "BIGQUERY": ("BIGQUERY_TO_EXASOL", "CONNECTION_NAME, IDENTIFIER_CASE_INSENSITIVE, PROJECT_ID, SCHEMA_FILTER, TABLE_FILTER"),
    "DATABRICKS": ("DATABRICKS_TO_EXASOL", "CONNECTION_NAME, CATALOG2SCHEMA, CATALOG_FILTER, SCHEMA_FILTER, TARGET_SCHEMA, TABLE_FILTER, IDENTIFIER_CASE_INSENSITIVE"),
    "SQLSERVER": ("SQLSERVER_TO_EXASOL", "CONNECTION_NAME, DB2SCHEMA, DB_FILTER, SCHEMA_FILTER, TARGET_SCHEMA, TABLE_FILTER, IDENTIFIER_CASE_INSENSITIVE"),
    "SNOWFLAKE": ("SNOWFLAKE_TO_EXASOL", "CONNECTION_NAME, DB2SCHEMA, DB_FILTER, SCHEMA_FILTER, TARGET_SCHEMA, TABLE_FILTER, IDENTIFIER_CASE_INSENSITIVE"),
    "ORACLE": ("ORACLE_TO_EXASOL", "CONNECTION_NAME, IDENTIFIER_CASE_INSENSITIVE, SCHEMA_FILTER, TABLE_FILTER, PARALLEL_STATEMENTS, CREATE_PK, CREATE_FK, CHECK_MIGRATION"),
    "TERADATA": ("TERADATA_TO_EXASOL", "CONNECTION_NAME, IDENTIFIER_CASE_INSENSITIVE, SCHEMA_FILTER, TABLE_FILTER, CHECK_MIGRATION"),
    "EXASOL": ("EXASOL_TO_EXASOL", "CONNECTION_NAME, CONNECTION_TYPE, IDENTIFIER_CASE_INSENSITIVE, SCHEMA_FILTER, TABLE_FILTER, GENERATE_VIEWS, VIEW_FILTER, PK_SETTING"),
    "NETEZZA": ("NETEZZA_TO_EXASOL", "CONNECTION_NAME, DB_FILTER, SCHEMA_FILTER, TABLE_FILTER, IDENTIFIER_CASE_INSENSITIVE"),
    "VECTORWISE": ("VECTORWISE_TO_EXASOL", "CONNECTION_NAME, IDENTIFIER_CASE_INSENSITIVE, TABLE_FILTER"),
}


def extract_create_script(path: Path, script_name: str) -> str:
    content = path.read_text()
    pattern = rf"create or replace script database_migration\.{script_name}\(.*?\n/\n"
    match = re.search(pattern, content, flags=re.IGNORECASE | re.DOTALL)
    if not match:
        raise AssertionError(f"Could not extract {script_name} from {path}")
    return match.group(0)


def sql_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def connect() -> pyexasol.ExaConnection:
    return pyexasol.connect(
        dsn=os.environ.get("EXA_DSN", "localhost:8566"),
        user=os.environ.get("EXA_USER", "sys"),
        password=os.environ.get("EXA_PASSWORD", "exasol"),
        encryption=os.environ.get("EXA_ENCRYPTION", "true").lower() != "false",
        websocket_sslopt={"cert_reqs": ssl.CERT_NONE},
    )


def mock_adapter_sql(script_name: str, params: str, target_schema: str) -> str:
    table_name = script_name.replace("_TO_EXASOL", "")
    return f"""
create or replace script database_migration.{script_name}({params}) returns table
as
if TABLE_FILTER == 'ADAPTER_FAIL' then
    error('mock adapter failed')
end

if TABLE_FILTER == 'EMPTY' then
    return {{
        {{'-- MOCK {script_name} EMPTY'}}
    }}, 'SQL_TEXT VARCHAR(2000000)'
end

return {{
    {{'-- MOCK {script_name}'}},
    {{'create schema if not exists "{target_schema}";'}},
    {{'create or replace table "{target_schema}"."{table_name}"("C" DECIMAL(18,0));'}},
    {{'insert into "{target_schema}"."{table_name}" values 1;'}}
}}, 'SQL_TEXT VARCHAR(2000000)'
/
"""


def wrapper_call(source: str, debug: bool, table_filter: str = "TBL") -> str:
    return (
        "execute script database_migration.MIGRATE_TO_EXASOL("
        f"{sql_string(source)},"
        "'MOCK_CONN',"
        "'JDBC',"
        "'DB',"
        "'SCH',"
        f"{sql_string(table_filter)},"
        "'TARGET_SCHEMA',"
        "TRUE,"
        f"{'TRUE' if debug else 'FALSE'},"
        "'PROJECT_ID=PROJECT;CATALOG2SCHEMA=true'"
        ")"
    )


def assert_contains(text: str, expected: str) -> None:
    if expected not in text:
        raise AssertionError(f"Expected {expected!r} in {text!r}")


def deploy_runtime_objects(conn: pyexasol.ExaConnection, target_schema: str) -> None:
    conn.execute("create schema if not exists database_migration")
    conn.execute(extract_create_script(REPO / "migrate_to_exasol.sql", "MIGRATE_TO_EXASOL"))
    for script_name, params in ADAPTERS.values():
        conn.execute(mock_adapter_sql(script_name, params, target_schema))


def main() -> int:
    target_schema = "MIGRATE_WRAPPER_RT_" + str(int(time.time()))
    conn = connect()
    deploy_runtime_objects(conn, target_schema)

    for source, (script_name, _) in ADAPTERS.items():
        rows = conn.execute(wrapper_call(source, debug=True)).fetchall()
        assert rows, f"{source} preview returned no rows"
        if rows[0][0] != "INFO":
            raise AssertionError(f"{source} first row STEP_KIND expected INFO, got {rows[0][0]}")
        assert_contains(rows[0][5], f"MOCK {script_name}")
        if rows[-1][0] != "SUMMARY" or rows[-1][4] != "PREVIEW":
            raise AssertionError(f"{source} missing SUMMARY/PREVIEW tail row: {rows[-1]}")
    print(f"preview_dispatch={len(ADAPTERS)}")

    for source in ("MYSQL", "SNOWFLAKE", "DATABRICKS", "ORACLE"):
        rows = conn.execute(wrapper_call(source, debug=False)).fetchall()
        summary = rows[-1]
        if summary[0] != "SUMMARY" or summary[4] != "OK":
            raise AssertionError(f"{source} did not report SUMMARY/OK: {summary}")
        script_name = ADAPTERS[source][0]
        table_name = script_name.replace("_TO_EXASOL", "")
        count = conn.execute(f'select count(*) from "{target_schema}"."{table_name}"').fetchval()
        if count != 1:
            raise AssertionError(f"{source} expected 1 loaded row, got {count}")
        kinds = {row[0] for row in rows}
        for required in ("CREATE_SCHEMA", "CREATE_TABLE", "IMPORT", "SUMMARY"):
            if required not in kinds:
                raise AssertionError(f"{source} missing STEP_KIND={required}; got {kinds}")
    print("execute_representative=4")

    rows = conn.execute(wrapper_call("MYSQL", debug=False, table_filter="EMPTY")).fetchall()
    summary = rows[-1]
    if summary[0] != "SUMMARY" or summary[1] != "No executable SQL generated" or summary[4] != "SKIPPED":
        raise AssertionError(f"Expected no-op SUMMARY row, got {summary}")
    print("empty_execution=pass")

    try:
        conn.execute(wrapper_call("MYSQL", debug=True, table_filter="ADAPTER_FAIL")).fetchall()
        raise AssertionError("Adapter failure did not raise")
    except Exception as exc:
        message = str(exc)
        assert_contains(message, "mock adapter failed")
        assert_contains(message, "MYSQL_TO_EXASOL")
    print("adapter_error=pass")

    try:
        conn.execute(wrapper_call("S3", debug=True)).fetchall()
        raise AssertionError("S3 did not raise")
    except Exception as exc:
        assert_contains(str(exc), "S3 is not supported by MIGRATE_TO_EXASOL")
    print("s3_rejection=pass")

    rows = conn.execute(wrapper_call("DATABRICKS_SQL", debug=True)).fetchall()
    assert_contains(rows[0][5], "MOCK DATABRICKS_TO_EXASOL")
    if rows[-1][0] != "SUMMARY":
        raise AssertionError(f"Alias dispatch missing SUMMARY: {rows[-1]}")
    print("alias_dispatch=pass")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
