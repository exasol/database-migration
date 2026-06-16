# Post load optimizations

This folder contains scripts that can be used after having imported data from another database.
What they do:
- Optimize the column's datatypes to minimize storage space on disk and to speed up joins, see section
- Import primary keys from other databases


## Table of Contents
1. [Optimize datatypes](#optimize_datatypes)
2. [Migrate primary keys](#migrate_primary_keys)


## Optimize datatypes


The `convert_datatypes.sql` script optimizes table column datatypes to reduce disk
storage and improve join performance. You typically run it once after importing your
data. It first reports (or applies) the changes it would make, so you stay in control.

> 📖 **Background:** see Exasol's [Performance Best Practices](https://docs.exasol.com/db/latest/performance/best_practices.htm).
> Choosing the smallest sufficient data type improves compression and join/query performance —
> that is exactly what this script helps you do.

Only **real, local base TABLE** columns are inspected:
- views and synonyms are excluded (`COLUMN_OBJECT_TYPE = 'TABLE'`)
- **virtual schema** columns are excluded (`COLUMN_IS_VIRTUAL = FALSE`)

> #### ⚠️⚠️ ATTENTION — `apply_conversion = true` IS IRREVERSIBLE ⚠️⚠️
>
> **`apply_conversion = true` runs `ALTER TABLE ... MODIFY` against your real tables.
> There is NO undo and NO automatic backup.**
>
> - **ALWAYS** run with `apply_conversion = false` first and carefully **REVIEW every proposed statement**.
> - Only set `true` when you are **100% sure** that every single proposed conversion must be performed.
> - **✅ Safer way:** keep `apply_conversion = false`, copy the statements from the `query_text`
>   column, and execute them **yourself, one statement at a time**, checking each result.
> - Make sure you have a **backup** / can recreate the affected tables before applying.

### Conversion types

| From | To | When |
|------|----|------|
| `DOUBLE` | smallest fitting `DECIMAL(p,0)` or `DECIMAL(p,s)` | the values are **exactly** representable as a DECIMAL: pure integers → `DECIMAL(9,0)`/`DECIMAL(18,0)`, or values with a small constant number of decimals (e.g. prices `19.99`) → `DECIMAL(p,s)`. Only proposed when a **round-trip cast proves it is lossless for every value** (`cast(cast(v as decimal(36,s)) as double) = v`); genuine floating-point values (e.g. `1/3`) stay `DOUBLE`. The detected scale is capped at 9 and the result never exceeds 64-bit. Chosen directly in a **single** `ALTER`. |
| `DECIMAL(p,0)` | `DECIMAL(9,0)` / `DECIMAL(18,0)` | the values fit into a smaller integer type. `9` maps to the 32-bit and `18` to the 64-bit internal representation. Example: `DECIMAL(20,0)` with max length 17 → `DECIMAL(18,0)`. |
| `DECIMAL(p,s)` | `DECIMAL(9,s)` / `DECIMAL(18,s)` | the required precision (integer digits + scale) fits into a smaller type; the **scale is preserved**. |
| `TIMESTAMP` / `TIMESTAMP WITH LOCAL TIME ZONE` | `DATE` | every value has a midnight time component (no hours/minutes/seconds/fraction), for any fractional precision `p` (0–9). For `TIMESTAMP WITH LOCAL TIME ZONE` the check and the conversion both run in the current `SESSIONTIMEZONE`. |
| `VARCHAR(n)` | smaller `VARCHAR` (same charset) | the actual maximum length plus a ~20% buffer (rounded up to the next round number) is smaller than `n`. Columns with `n <= 3` are left untouched. The original **character set (`ASCII` / `UTF8`) is preserved** — e.g. `VARCHAR(2000000) ASCII` becomes `VARCHAR(500) ASCII`, never `UTF8`. |

> The script does not change a column when the table/column is empty (all NULL), when
> the value is a real `DOUBLE`/`TIMESTAMP`, or when no smaller type fits.

### Parameters

```sql
execute script DATABASE_MIGRATION.CONVERT_DATATYPES(
    'MY_SCHEMA',   -- schema_name:       schema name or filter, wildcards (%) allowed
    '%',           -- table_name:        table name or filter, wildcards (%) allowed
    true,          -- convert_double:    DOUBLE       -> smallest fitting DECIMAL(p,0) / DECIMAL(p,s)
    true,          -- convert_integer:   DECIMAL(p,0) -> DECIMAL(9,0) / DECIMAL(18,0)
    true,          -- convert_decimal:   DECIMAL(p,s) -> DECIMAL(9,s) / DECIMAL(18,s)
    true,          -- convert_timestamp: TIMESTAMP / TIMESTAMP WITH LOCAL TIME ZONE -> DATE
    true,          -- convert_varchar:   VARCHAR(n)   -> smaller VARCHAR (same charset)
    false,         -- log_for_all_columns: false = only report changed columns, true = report every inspected column
    false          -- apply_conversion:  false = only report (recommended), true = IRREVERSIBLY apply
);
```

| Parameter | Description |
|-----------|-------------|
| `SCHEMA_NAME` | The schema you want to modify. Wildcards (`%`) are allowed. The filter is a value comparison against the case-exact catalog name — see *Case sensitivity* below. |
| `TABLE_NAME` | The table you want to modify. Wildcards (`%`) are allowed. |
| `CONVERT_DOUBLE` | `true`/`false` — check & convert `DOUBLE` → `DECIMAL(p,0)` / `DECIMAL(p,s)`. |
| `CONVERT_INTEGER` | `true`/`false` — check & convert `DECIMAL(p,0)` → `DECIMAL(9,0)` / `DECIMAL(18,0)`. |
| `CONVERT_DECIMAL` | `true`/`false` — check & convert `DECIMAL(p,s)` → `DECIMAL(9,s)` / `DECIMAL(18,s)`. |
| `CONVERT_TIMESTAMP` | `true`/`false` — check & convert `TIMESTAMP` / `TIMESTAMP WITH LOCAL TIME ZONE` → `DATE`. |
| `CONVERT_VARCHAR` | `true`/`false` — check & convert `VARCHAR(n)` → smaller `VARCHAR` (same charset). |
| `LOG_FOR_ALL_COLUMNS` | `true`/`false`. `false` = report only columns that **will change**. `true` = report **every inspected column**, including `Keep ...` rows (useful to see *why* a column is left unchanged). |
| `APPLY_CONVERSION` | 🛑 **`false` = only report the proposed changes — STRONGLY RECOMMENDED.** `true` = **IRREVERSIBLY** execute the `ALTER TABLE` statements against your tables — **no undo**. Only use `true` after reviewing the dry-run; see the ⚠️ warning box above. |

Each of the five `convert_*` switches enables one conversion type independently; a type set to
`false` is neither inspected nor applied. They apply to both the dry-run and the apply run.

### Output

What is reported depends on the `log_for_all_columns` parameter:
- `log_for_all_columns = false` → **one row per column that will change**.
- `log_for_all_columns = true`  → **one row per inspected column**, including `Keep ...` rows
  that explain why a column is left unchanged.

The output is **sorted by `schema_name`, `table_name`, `column_name`**.

| Column | Meaning |
|--------|---------|
| `schema_name`, `table_name`, `column_name` | the inspected column |
| `conversion` | what happens, e.g. `DOUBLE --> DECIMAL(9, 0), max length: 1`, `VARCHAR(1000) UTF8  --> VARCHAR(20) UTF8, max length: 10`, or `Keep VARCHAR(100) UTF8, max length: 12` |
| `query_text` | the generated `ALTER TABLE ... MODIFY ...` statement |
| `success` | only present when `apply_conversion = true`: `true` or the error message |

If the result would be empty, the script returns a single informative row in the `conversion`
column instead:
- `log_for_all_columns = false` → `No columns found that need optimization.`
- `log_for_all_columns = true`  → `No matching columns found (check the filters and the convert_* switches).`
  — i.e. no column matched the schema/table filter and the enabled `convert_*` switches at all.

### How it works

- **Single scan per column.** Each column is measured with one aggregate query (not-null
  count plus the relevant length/flag information), so a table is scanned once per column.
- **DOUBLE in one step, only when lossless.** A convertible `DOUBLE` is changed directly
  to its optimal `DECIMAL(p,0)` (integers) or `DECIMAL(p,s)` (constant-scale decimals) in a
  single statement. The decision uses a **round-trip cast** — `cast(cast(v as decimal(36,s))
  as double) = v` must hold for every row — which guarantees the conversion does not alter
  any value. Genuine floating-point values stay `DOUBLE`.
- **Parameter-independent timestamp check.** The `TIMESTAMP -> DATE` detection uses
  `TRUNC()` and therefore does not depend on the `TIMESTAMP_ARITHMETIC_BEHAVIOR`
  database parameter. It covers both `TIMESTAMP` and `TIMESTAMP WITH LOCAL TIME ZONE`
  of any precision `p` (0–9); for the time-zone variant the check and conversion are
  evaluated in the current `SESSIONTIMEZONE`.

### Case sensitivity

Exasol folds **unquoted** identifiers to UPPER CASE, while **delimited** (double-quoted)
identifiers are case-sensitive (default `SQL_IDENTIFIER_COMPARISON = CASE SENSITIVE`).
The script processes every schema/table/column name as a delimited identifier (via
`quote()` and the `::identifier` placeholder), so `MixedCase` and `lowercase` names are
handled correctly.

The `schema_name` / `table_name` **filter**, however, is a value comparison against the
names as stored in the catalog. Pass the name exactly as it was created (e.g.
`'MixedCase'`, not `'MIXEDCASE'`), or use `%`.

### Recommended workflow

1. Run with `apply_conversion = false` and **carefully review every proposed `ALTER` statement**.
2. Then choose one:
   - **✅ Safest (recommended):** keep `apply_conversion = false`, copy the statements from the
     `query_text` column, and run them **yourself, one statement at a time**, checking each result.
   - 🛑 Or set `apply_conversion = true` — **only if you are 100% sure**; this applies *all* changes
     irreversibly in one go, with no undo.




## Migrate primary keys

See script [set_primary_keys.sql](set_primary_keys.sql)
