# Post load optimizations

This folder contains scripts that can be used after having imported data from another database.
What they do:
- **Convert VARCHAR columns** that actually hold numbers / dates / booleans / … into their real data
  types (`convert_varchar`)
- **Optimize the column data types** to minimize storage space on disk and to speed up joins
  (`convert_datatypes`)
- **Import primary keys** from other databases (`set_primary_keys`)

> 🔁 **Recommended order:** run [`convert_varchar`](#convert_varchar) **first** (turn string columns into
> their real data types), then [`convert_datatypes`](#optimize_datatypes) to shrink everything to the
> smallest sufficient size.


## Table of Contents
1. [Convert VARCHAR columns](#convert_varchar)
2. [Optimize datatypes](#optimize_datatypes)
3. [Migrate primary keys](#migrate_primary_keys)


## Convert VARCHAR columns to real data types <a name="convert_varchar"></a>

The `convert_varchar.sql` script inspects the **`VARCHAR` columns** that match a schema/table filter and,
based on a **sample** of the actual values, suggests the smallest/most appropriate data type for each
column. It is meant for the typical post-import situation where everything arrived as `VARCHAR`.

> 📖 **Background:** see Exasol's [Performance Best Practices](https://docs.exasol.com/db/latest/performance/best_practices.htm) —
> using the smallest sufficient data type improves compression and join/query performance.

> 🔁 **Recommended order: run `convert_varchar` FIRST, then `convert_datatypes`.**
> `convert_varchar` turns string columns into their *true* types (`VARCHAR` → `DECIMAL`/`DATE`/
> `TIMESTAMP`/`BOOLEAN`/…). Afterwards run [`convert_datatypes`](#optimize_datatypes) to squeeze those
> (and the existing numeric/timestamp/varchar columns) down to the smallest sufficient size. Doing it the
> other way round misses the columns that are still `VARCHAR`.

Only **real, local base TABLE** columns are inspected (views/synonyms and **virtual schema** columns are
excluded: `COLUMN_OBJECT_TYPE = 'TABLE'` and `COLUMN_IS_VIRTUAL = FALSE`).

> ### 🌐 NLS settings & formats
> Two detection paths:
> 1. **Primary (session NLS).** `IS_DATE` / `IS_TIMESTAMP` / `IS_NUMBER` and the plain `ALTER`s use the
>    current session settings (`NLS_DATE_FORMAT`, `NLS_TIMESTAMP_FORMAT`, `NLS_NUMERIC_CHARACTERS`,
>    `NLS_DATE_LANGUAGE`) — a value is recognized here only if it matches the session format. The decimal
>    separator is read from `NLS_NUMERIC_CHARACTERS` (so German `1234,56` works under `',.'`; no longer
>    hard-coded `.`). `NLS_DATE_LANGUAGE` only matters for month/day **names**; `NLS_FIRST_DAY_OF_WEEK` is
>    irrelevant.
> 2. **Multi-format fallback (NLS-independent).** Columns not classified by path 1 are probed against a
>    set of explicit format models — so a German `12.06.2026` (or `DD.MM.YYYY HH24:MI:SS`) is detected
>    **regardless of the session NLS** (see *Multi-format detection* below).
>
> Note: the suggested **plain** `MODIFY COLUMN … DATE/TIMESTAMP/DECIMAL` (path 1) parses via the session
> NLS — so apply it in a session whose NLS matches the data. The multi-format suggestions (path 2) already
> include the matching `ALTER SESSION SET NLS_…_FORMAT='…'` so they are self-contained.

### Multi-format detection (explicit format models)

For columns that the session-NLS path does not classify, the script probes a set of common explicit
formats and, when **exactly one** matches **all** sampled values, prints a self-contained recipe:

| Example values | Output |
|---|---|
| `12.06.2026`, `31.12.2025` | `ALTER SESSION SET NLS_DATE_FORMAT='DD.MM.YYYY';` + `… MODIFY COLUMN c DATE;` |
| `06/15/2026` | `… NLS_DATE_FORMAT='MM/DD/YYYY';` + `… DATE;` |
| `2026.06.12` | `… NLS_DATE_FORMAT='YYYY.MM.DD';` + `… DATE;` |
| `12.06.2026 10:00:00.123456` | `… NLS_TIMESTAMP_FORMAT='DD.MM.YYYY HH24:MI:SS.FF6';` + `… TIMESTAMP(6);` |
| `01.02.2026`, `03.04.2026` (every day ≤ 12) | **AMBIGUOUS** warning — DD.MM vs MM.DD cannot be told apart, so it is *not* guessed; pick the format yourself |

Probed formats: date orders `YYYY/MM/DD`, `DD/MM/YYYY`, `MM/DD/YYYY` with separators `-`, `.`, `/`
(and the same with ` HH24:MI:SS[.FF]` for timestamps). The fractional-seconds precision (0–9) is detected
from the values. Day/month order is only chosen when the data disambiguates it (some day > 12); otherwise
it is reported as ambiguous. Time-of-day must be `HH24:MI:SS[.FF]` (24-hour); other time formats are not
auto-detected.

> ### ⚠️ REPORT ONLY — and review before you apply anything
> This script **does not change your data**. It only **returns rows** describing the suggestion plus the
> `ALTER TABLE ... MODIFY COLUMN` statement(s) you *could* run (in the `query_text` column). You execute
> them yourself.
>
> - Decisions are based on a **SAMPLE** (see `sample_size`). A value **outside** the sample may not fit
>   the suggested type, so a generated `ALTER` can still fail or change data.
> - Numeric/date suggestions can be **LOSSY**: e.g. `'007' → 7` (leading zeros lost), `'+49' → 49`
>   (sign/format lost). Zip codes, phone numbers and article numbers are typical traps.
> - **Always review each statement** and ideally run them one by one, checking the result.
>
> The output also flags risky cases **per column, in the `notes` column**:
> - a numeric column with **leading zeros or a `+` sign** → `WARNING: looks like an identifier … LOSES them … Review!`
> - `0/1` → `BOOLEAN` and `TRUE/FALSE` → `BOOLEAN` → a `NOTE:` to verify it is really a boolean (not flags/codes).

### What it detects

| If the sampled values look like … | Suggestion |
|---|---|
| integers | `DECIMAL(p)` (precision rounded up to 9 / 18 / 36) |
| integers + decimals | `DECIMAL(p, s)` (precision **p** rounded up to 9 / 18 / 36; scale **s** kept) |
| any numeric incl. scientific notation | `DOUBLE PRECISION` |
| dates only (no time component) | `DATE` |
| dates and/or timestamps | `TIMESTAMP(p)` with the **detected fractional-seconds precision** `p` (0–9), so micro/nanosecond values are not truncated; + hint to consider `TIMESTAMP WITH LOCAL TIME ZONE` |
| `TRUE`/`FALSE`, or only `0`/`1` | `BOOLEAN` |
| day-to-second intervals | `INTERVAL DAY(p) TO SECOND(fp)` |
| year-to-month intervals | `INTERVAL YEAR(p) TO MONTH` |
| WKT geometry (`POINT (…)`, `POLYGON (…)`, …) | `GEOMETRY` (+ hint to specify an SRID) |
| nothing single fits, but values are shorter than the column | shrink to a smaller `VARCHAR` — actual max length **+ ~20% headroom, rounded up**, **character set preserved**. Columns with **n ≤ 3** are left untouched |
| the **column name** looks like a date/timestamp but values don't parse | a hint with an example `UPDATE`/`ALTER` |
| the column is empty (in the sample) | a hint that the column could be dropped |

### Parameters

```sql
execute script DATABASE_MIGRATION.CONVERT_VARCHAR(
    'MY_SCHEMA',   -- schema_pattern:      schema name or filter, wildcards (%) allowed
    '%',           -- table_pattern:       table name or filter, wildcards (%) allowed
    '5%',          -- sample_size:         number of rows (min 1000) or a percentage string like '5%'
    false          -- log_for_all_columns: false = only columns that change, true = every inspected column
);
```

| Parameter | Description |
|-----------|-------------|
| `SCHEMA_PATTERN` | Schema name or filter (`%` allowed). Must be a non-empty string. |
| `TABLE_PATTERN` | Table name or filter (`%` allowed). Must be a non-empty string. |
| `SAMPLE_SIZE` | How many rows to inspect per table: an integer (number of rows, minimum 1000) **or** a percentage string like `'5%'`. Anything else defaults to `1%`. **A 1–5% sample is usually statistically sufficient** for a reliable type guess and is much faster on large tables; use `'100%'` only when you must check every single value. |
| `LOG_FOR_ALL_COLUMNS` | `true`/`false`. `false` = report only columns that get a **suggestion** (a conversion or shrink). `true` = report **every inspected** `VARCHAR` column, including `Keep VARCHAR(…)` rows and advisory rows (ambiguous formats, date-name hints, empty columns). |

### Output

The script returns **structured rows**, sorted by `schema_name`, `table_name`, `column_name`, with a
`notes` column for the warnings/hints/recipes:

| Column | Meaning |
|--------|---------|
| `schema_name`, `table_name`, `column_name` | the inspected column |
| `conversion` | short description of the suggestion, e.g. `VARCHAR(50) UTF8 --> DECIMAL(9, 0)` or `Keep VARCHAR(100) UTF8, max length: 12` |
| `query_text` | the `ALTER` statement(s) you would run (empty for *keep*/advisory rows). Multi-format date/timestamp suggestions include the required `ALTER SESSION SET NLS_..._FORMAT='…';` **before** the `ALTER TABLE`, so the cell is runnable as-is |
| `notes` | warnings (leading zeros / `+` sign), the `0/1`-boolean `NOTE`, the ambiguity message, the TIMESTAMP-precision recipe, column-name hints |

`log_for_all_columns = false` returns only columns that get a suggestion; `= true` returns **every**
inspected column (incl. `Keep …` rows and advisory rows).

Example (`log_for_all_columns = true`, session NLS at the ISO default):

| schema_name | table_name | column_name | conversion | query_text | notes |
|---|---|---|---|---|---|
| MY_SCHEMA | CUSTOMERS | ACTIVE | `VARCHAR(10) UTF8 --> BOOLEAN` | `ALTER TABLE "MY_SCHEMA"."CUSTOMERS" MODIFY COLUMN "ACTIVE" BOOLEAN;` | NOTE: only 0/1 values. Verify these are real booleans, not flags/bits/codes you compute with. |
| MY_SCHEMA | CUSTOMERS | AMOUNT | `VARCHAR(20) UTF8 --> DECIMAL(9, 2)` | `ALTER TABLE "MY_SCHEMA"."CUSTOMERS" MODIFY COLUMN "AMOUNT" DECIMAL(9, 2);` | |
| MY_SCHEMA | CUSTOMERS | COMMENT | `VARCHAR(2000000) UTF8 --> VARCHAR(20) UTF8, max length: 15` | `ALTER TABLE "MY_SCHEMA"."CUSTOMERS" MODIFY COLUMN "COMMENT" VARCHAR(20) UTF8;` | Mixed values; no single type fits. Shrinking the width (actual max length 15 + ~20% headroom); character set preserved. |
| MY_SCHEMA | CUSTOMERS | CUST_ID | `VARCHAR(50) UTF8 --> DECIMAL(9, 0)` | `ALTER TABLE "MY_SCHEMA"."CUSTOMERS" MODIFY COLUMN "CUST_ID" DECIMAL(9, 0);` | WARNING: some values have leading zeros or a '+' sign (looks like an identifier: ID / ZIP / phone / article no.). Converting to DECIMAL LOSES them ('007' -> 7, '+49' -> 49). Review before applying! |
| MY_SCHEMA | CUSTOMERS | DE_DATE | `VARCHAR(20) UTF8 --> DATE (format DD.MM.YYYY)` | `ALTER SESSION SET NLS_DATE_FORMAT='DD.MM.YYYY'; ALTER TABLE "MY_SCHEMA"."CUSTOMERS" MODIFY COLUMN "DE_DATE" DATE;` | Values match the date format 'DD.MM.YYYY' (not the session NLS_DATE_FORMAT). Run BOTH statements in query_text (the ALTER SESSION first). |
| MY_SCHEMA | CUSTOMERS | ORDER_DATE | `VARCHAR(20) UTF8 --> DATE` | `ALTER TABLE "MY_SCHEMA"."CUSTOMERS" MODIFY COLUMN "ORDER_DATE" DATE;` | |

If no `VARCHAR` column matches the filter (no such table, or the table has no `VARCHAR` columns), the
script returns a single informative row instead of an empty result set (in the `conversion` column):

- `log_for_all_columns = false` → `No columns found that need optimization.`
- `log_for_all_columns = true`  → `No matching VARCHAR columns found (check the schema/table filter).`

### Foreign keys (handled automatically)

In Exasol a type change on a **primary/foreign key** column fails unless the linked PK and FK columns are
changed to the **same** type (`constraint violation … wrong types`). When `FOREIGN KEY`s touch the analyzed
tables, the script handles this for you:

- **DROP/RE-ADD wrapper:** the output gains a **`### DROP FOREIGN KEYS — run FIRST ###`** section and a
  **`### RE-ADD FOREIGN KEYS — run LAST ###`** section (composite FKs included), so the whole script runs end
  to end: drop the FKs, change the columns, re-add the FKs. Each FK is re-added in its **original
  `ENABLE`/`DISABLE` state** — the script never changes whether a constraint is enabled or disabled.
- **Type harmonization:** every referential key group (a PK column plus all FK columns linked to it,
  transitively) is converted to **one common target type that fits all of its columns** — the optimal common
  type (e.g. a 9-digit and a 12-digit key column → `DECIMAL(18,0)`), never a blanket `VARCHAR`. If the group
  has no common convertible type, it is kept unchanged (the FK stays valid) with a note.
- **Single table with an FK to another table:** if a key column's group reaches a table **outside the current
  filter**, the column is kept unchanged with a note to re-run with a `TABLE_FILTER` that also includes the
  related table(s) (so they are converted together to the same type).
- With **no** foreign keys in scope the run is exactly as before (one cheap catalog check is the only overhead).

The script remains **report-only** — it returns the statements; you review and run them in the shown order.

### How it works / notes

- **Type-directed, single scan.** Each column is analyzed with one aggregate query whose inner `CASE`
  classifies every value **once and short-circuits** — a numeric value is settled by `IS_NUMBER` alone and
  never runs the costly date/timestamp/interval/geometry checks, so the per-row cost matches the column's
  real type. When the sample covers the whole table (e.g. `'100%'`) the `LIMIT` subquery is **omitted**, so
  the table is scanned straight into the aggregation instead of first being copied into a temporary table.
  The NLS-independent multi-format probe is a separate query that runs **only for unclassified columns whose
  values are date-LIKE** (a cheap one-regex pre-check skips it for name/code/free-text columns) and that
  **also omits the `LIMIT`** on a full scan. The suggestion reflects the sample, not necessarily the whole column.
- **Sampling is the main cost lever.** A **1–5% sample is usually statistically sufficient** to infer a
  column's type reliably (often even `'1%'`), and it is dramatically faster than `'100%'` on large tables.
  Prefer such a sample (or a fixed row count) over `'100%'`; reserve `'100%'` for when you truly must verify
  that the suggested type fits *every* value. Avoid high-but-not-100% values like `'99%'` (they still
  materialize almost all rows). With a sample, only those rows are materialized.
- **Robust date/timestamp check.** Date vs. timestamp uses `IS_DATE` / `IS_TIMESTAMP` plus `TRUNC()`
  to detect a time component; `TO_TIMESTAMP()` is only evaluated for values that are timestamps, so the
  check does **not** depend on `TIMESTAMP_ARITHMETIC_BEHAVIOR` and works even when `NLS_DATE_FORMAT` and
  `NLS_TIMESTAMP_FORMAT` differ (e.g. a German `DD.MM.YYYY` date format).
- **TIMESTAMP precision (0–9).** The suggested `TIMESTAMP(p)` uses the detected number of fractional-
  second digits, so values down to nanoseconds are preserved (a bare `TIMESTAMP` is `TIMESTAMP(3)` and
  would truncate anything finer than milliseconds). If a column has **more** fractional digits than the
  session `NLS_TIMESTAMP_FORMAT` can parse (e.g. data has 9 digits but the format is `FF6`), the output
  warns that a plain `ALTER` would truncate and prints the `ALTER SESSION SET NLS_TIMESTAMP_FORMAT='…FF<p>'`
  recipe needed to keep full precision.
  `TIMESTAMP WITH LOCAL TIME ZONE` is only *suggested as a hint* (it cannot be told apart from a plain
  timestamp by the text alone).
- **Character set preserved.** When a column is only shrunk (`VARCHAR(n)`), its original `ASCII` / `UTF8`
  character set is kept (e.g. `VARCHAR(2000000) ASCII` → `VARCHAR(20) ASCII`, never `UTF8`).
- **Robust.** A single column or table that cannot be analyzed is reported and skipped.
- **Limitations.** Geometry is recognized by a WKT text pattern (heuristic, may yield false positives);
  numeric/date suggestions are lossy for identifier-like data (see the warning above).


## Optimize datatypes <a name="optimize_datatypes"></a>

The `convert_datatypes.sql` script optimizes table column datatypes to reduce disk
storage and improve join performance. You typically run it once after importing your
data. It first reports (or applies) the changes it would make, so you stay in control.

> 🔁 **Tip:** if your data is still in `VARCHAR` columns, run [`convert_varchar`](#convert_varchar) first
> to give those columns their real data types, then run this script to shrink everything to the smallest
> sufficient size.

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

### Why it matters: DECIMAL storage classes

Exasol stores a `DECIMAL` with precision ≤ 9 in 32 bit, ≤ 18 in 64 bit and above that in 128 bit.
Measured on 5 million rows (Exasol 2025.1.11 and 2025.1.16: same memory result, similar speed-up), moving money / count / key columns from `DECIMAL(36,18)`
to a 64-bit `DECIMAL(18,s)` reduced the compressed memory of the table about **3×** (103.9 MB → 33.5 MB),
made `GROUP BY` and `JOIN` queries on those columns about **1.7–1.9×** faster and the join index **4.4×**
smaller. Going on from 64 to 32 bit halves the raw size again but brings no further compressed-memory gain.
`DECIMAL(36,18)` is the default mapping of many migration and ETL tools — see *Scale reduction* below.

### Conversion types

| From | To | When |
|------|----|------|
| `DOUBLE` | smallest fitting `DECIMAL(p,0)` or `DECIMAL(p,s)` | the values are **exactly** representable as a DECIMAL: pure integers → `DECIMAL(9,0)`/`DECIMAL(18,0)`, or values with a small constant number of decimals (e.g. prices `19.99`) → `DECIMAL(p,s)`. Only proposed when a **round-trip cast against the real target type proves it is lossless for every value** (`cast(cast(v as decimal(p,s)) as double) = v`, tried with `p` = 9 first, then 18 — an exact check without tolerance); genuine floating-point values (e.g. `1/3`) stay `DOUBLE`. The detected scale is capped at 9 and the result never exceeds 64-bit. Chosen directly in a **single** `ALTER`. Like a scale reduction, the target is derived from today's values — see the warning below. |
| `DECIMAL(p,0)` | `DECIMAL(9,0)` / `DECIMAL(18,0)` | the values fit into a smaller integer type. `9` maps to the 32-bit and `18` to the 64-bit internal representation. Example: `DECIMAL(20,0)` with max length 17 → `DECIMAL(18,0)`. An `IDENTITY` column is never shrunk below `DECIMAL(18,0)` and always keeps at least one digit of headroom above its current counter value, so the counter can keep growing. |
| `DECIMAL(p,s)` | `DECIMAL(9,s)` / `DECIMAL(18,s)` | the required precision (integer digits + scale) fits into a smaller type; the **scale is preserved** (a value below 1 has 0 integer digits, so e.g. fraction-only data fits `DECIMAL(18,18)`). |
| `DECIMAL(p,s)` with `reduce_decimal_scale = true` | `DECIMAL(9,s')` / `DECIMAL(18,s')` with a **smaller scale** `s'` | every value uses fewer decimals than `s` **and** the smaller scale moves the column into a smaller storage class — see *Scale reduction* below. Example: `DECIMAL(36,18)` amounts using 1 or 2 decimals and at most 7 integer digits → `DECIMAL(9,2)`. |
| `TIMESTAMP` / `TIMESTAMP WITH LOCAL TIME ZONE` | `DATE` | every value has a midnight time component (no hours/minutes/seconds/fraction), for any fractional precision `p` (0–9), **and** the column has no `DEFAULT` that supplies a time of day (`CURRENT_TIMESTAMP`, `SYSTIMESTAMP`, `NOW`, …) and no `DEFAULT` written as a string (its meaning depends on the session's NLS date / timestamp format). For `TIMESTAMP WITH LOCAL TIME ZONE` the check and the conversion both run in the current `SESSIONTIMEZONE`. |
| `VARCHAR(n)` | smaller `VARCHAR` (same charset) | the actual maximum length plus a ~20% buffer (rounded up to the next round number) is smaller than `n`. Columns with `n <= 3` are left untouched. The original **character set (`ASCII` / `UTF8`) is preserved** — e.g. `VARCHAR(2000000) ASCII` becomes `VARCHAR(500) ASCII`, never `UTF8`. |

> The script does not change a column when the table/column is empty (all NULL) — except an empty foreign key
> column whose key group contains data: it gets the group's common type (a key group whose columns are all empty
> is kept) —, when the value is a real `DOUBLE`/`TIMESTAMP`, or when no smaller type fits.
>
> **Column `DEFAULT`s are taken into account:** a literal `DEFAULT` is treated like one more value of the
> column, so the target type always holds it (e.g. `VARCHAR` with `DEFAULT 'UNKNOWN'` is never shrunk below 7
> characters; numbers and `TRUE`/`FALSE` count on `VARCHAR` columns, too); `DEFAULT NULL` is the same as no
> default. A number written as a string (e.g. `'1.5'`, as MySQL-style DDL does) is converted again by
> `ALTER … MODIFY` with the session's NLS settings, so the conversion is only proposed when that works in the
> current session. A column with a non-literal `DEFAULT` such as `CURRENT_USER` or `'a' || 'b'` (on `DECIMAL`,
> `DOUBLE` or `VARCHAR`) is kept, and so is a `DOUBLE` whose literal `DEFAULT` has more digits than a `DOUBLE`
> holds. A `TIMESTAMP` with a `DEFAULT` that supplies (or may supply) a time of day, or that is written as a
> string, is never converted to `DATE`; a date default such as `CURRENT_DATE` or `DATE '2024-01-01'` is fine.

### Scale reduction (`reduce_decimal_scale`)

By default the scale of a `DECIMAL(p,s)` column is preserved. That keeps `DECIMAL(36,18)` at 128 bit as soon as
any value has an integer digit (integer digits + 18 is then more than 18); only fraction-only columns still go to
`DECIMAL(18,18)`. With `reduce_decimal_scale = true` (together with
`convert_decimal = true`) the script additionally reduces the scale — **losslessly, never by rounding**.

> ✅ **Recommended: `reduce_decimal_scale = true`.** It is the only way to shrink the common `DECIMAL(36,18)`
> columns, and the reduction itself never changes a value. Before applying, read the ⚠️ **WARNING** below and
> review the `SCALE REDUCTIONS` section of the dry-run output: after a scale reduction, later loads with more
> decimals are rounded silently. Use `false` for columns whose source may deliver more decimals later.

How it works:

1. The number of decimals every value actually uses is measured **exactly** (`value = TRUNC(value, s)`,
   exact DECIMAL arithmetic) in the same single scan of the column.
2. The new scale is the next step of the ladder **0 / 2 / 4 / 6 / 9 / 12 / 18** at or above that number, never
   above the current scale. Amounts whose values currently only end in `.0`/`.5` therefore get scale 2, not 1.
3. The precision is the smallest of 9 / 18 that holds integer digits + new scale.
4. The scale is **only** changed when that reaches a smaller storage class than keeping it (128 → 64/32 bit,
   64 → 32 bit). Columns whose values use all their decimals keep their scale — typically data that was loaded
   from `DOUBLE` into `DECIMAL(36,18)` and carries binary noise (e.g. `12.959999999983222784`); their precision is
   still reduced when integer digits + the current scale fit (e.g. fraction-only data → `DECIMAL(18,18)`). Clean
   such data first if the extra decimals are noise.

All scale reductions are listed in their own output section **`SCALE REDUCTIONS`**, marked `[SCALE REDUCED]`.

> #### ⚠️ Silent rounding of later loads
> After a scale reduction — and likewise after a `DOUBLE` → `DECIMAL(p,s)` conversion — Exasol **rounds silently** every later `INSERT` / `IMPORT` / `MERGE` / `UPDATE`
> value that has more decimals than the new scale (e.g. `7.777` → `7.78` for scale 2); values with too many
> integer digits are rejected with an error. Only reduce the scale when you know that the source never
> delivers more decimals. Views on the column keep working but return narrower `DECIMAL` types, and `DOUBLE`
> results computed from it (e.g. `value / 3`, `AVG`) can differ in the last binary digit.

### Parameters

```sql
execute script DATABASE_MIGRATION.CONVERT_DATATYPES(
    'MY_SCHEMA',   -- schema_name:          schema name (exact) or a LIKE pattern with %
    '%',           -- table_name:           table name (exact) or a LIKE pattern with %
    true,          -- convert_double:       DOUBLE       -> smallest fitting DECIMAL(p,0) / DECIMAL(p,s)
    true,          -- convert_integer:      DECIMAL(p,0) -> DECIMAL(9,0) / DECIMAL(18,0)
    true,          -- convert_decimal:      DECIMAL(p,s) -> DECIMAL(9,s) / DECIMAL(18,s), scale kept
    true,          -- reduce_decimal_scale: true (recommended) = ALSO reduce the scale losslessly, e.g. DECIMAL(36,18) -> DECIMAL(9,2) (see WARNING)
    true,          -- convert_timestamp:    TIMESTAMP / TIMESTAMP WITH LOCAL TIME ZONE -> DATE
    true,          -- convert_varchar:      VARCHAR(n)   -> smaller VARCHAR (same charset)
    false,         -- log_for_all_columns:  false = only report changed columns, true = report every inspected column
    false          -- apply_conversion:     false = only report (recommended), true = IRREVERSIBLY apply (all-or-nothing)
);
```

| Parameter | Description |
|-----------|-------------|
| `SCHEMA_NAME` | The schema you want to modify. A value **without an unescaped `%`** is an **exact** name (an `_` in it is an ordinary character); a value **with** `%` is a `LIKE` pattern, where `_` matches any single character. A backslash escapes a following `%`, `_` or backslash in both forms (`MY\_SCHEMA` = `MY_SCHEMA`, `MY\_SCHEMA%` = names starting with `MY_SCHEMA`); any other backslash is an ordinary character. The comparison is case-exact — see *Case sensitivity* below. |
| `TABLE_NAME` | The table you want to modify — exact name or `LIKE` pattern, same rules as `SCHEMA_NAME`. |
| `CONVERT_DOUBLE` | `true`/`false` — check & convert `DOUBLE` → `DECIMAL(p,0)` / `DECIMAL(p,s)`. |
| `CONVERT_INTEGER` | `true`/`false` — check & convert `DECIMAL(p,0)` → `DECIMAL(9,0)` / `DECIMAL(18,0)`. |
| `CONVERT_DECIMAL` | `true`/`false` — check & convert `DECIMAL(p,s)` → `DECIMAL(9,s)` / `DECIMAL(18,s)` (scale kept). |
| `REDUCE_DECIMAL_SCALE` | `true`/`false` — **`true` recommended** — only with `CONVERT_DECIMAL = true`: additionally reduce the scale losslessly when that reaches a smaller storage class (see *Scale reduction* and its ⚠️ **WARNING**: later loads with more decimals are rounded silently). `false` keeps every scale. |
| `CONVERT_TIMESTAMP` | `true`/`false` — check & convert `TIMESTAMP` / `TIMESTAMP WITH LOCAL TIME ZONE` → `DATE`. |
| `CONVERT_VARCHAR` | `true`/`false` — check & convert `VARCHAR(n)` → smaller `VARCHAR` (same charset). |
| `LOG_FOR_ALL_COLUMNS` | `true`/`false`. `false` = report only columns that **will change** (plus every column whose check failed — a failed check is never hidden — and the foreign key note for key columns whose group reaches a table outside the filter). `true` = report **every inspected column**, including `Keep ...` rows (useful to see *why* a column is left unchanged). |
| `APPLY_CONVERSION` | 🛑 **`false` = only report the proposed changes — STRONGLY RECOMMENDED.** `true` = **IRREVERSIBLY** execute the statements against your tables (all-or-nothing, see below). Only use `true` after reviewing the dry-run; see the ⚠️ warning box above. |

Each of the five `convert_*` switches enables one conversion type independently; a type set to
`false` is neither inspected nor applied. They apply to both the dry-run and the apply run. Only an explicit
`true` switches a parameter on — `NULL` counts as `false` (so a `NULL` in `apply_conversion` never applies).

> **Upgrading from the 9-parameter version:** the new `reduce_decimal_scale` parameter sits right after
> `convert_decimal`. Existing calls with 9 arguments fail with *expected 10 script parameters* — add `true`
> in the 6th position (recommended, see *Scale reduction*) or `false` to keep the previous behaviour.

### Output

What is reported depends on the `log_for_all_columns` parameter:
- `log_for_all_columns = false` → **one row per column that will change**, plus a `Keep … (check failed: <error>)`
  row for every column whose check query failed and a `Keep …` note row for every key column whose foreign key
  group reaches a table outside the filter (see *Foreign keys*).
- `log_for_all_columns = true`  → **one row per inspected column**, including `Keep ...` rows
  that explain why a column is left unchanged.

The output is **sorted by `schema_name`, `table_name`, `column_name`**; scale reductions follow in their own
`SCALE REDUCTIONS` section.

| Column | Meaning |
|--------|---------|
| `schema_name`, `table_name`, `column_name` | the inspected column |
| `conversion` | the **exact current type** and **exact target type**, e.g. `DECIMAL(20, 0) --> DECIMAL(9, 0), max length: 1`, `DECIMAL(36, 18) --> DECIMAL(9, 2) [SCALE REDUCED], integer digits: 6, decimals used: 2`, `TIMESTAMP(6) WITH LOCAL TIME ZONE --> DATE`, `VARCHAR(1000) UTF8 --> VARCHAR(20) UTF8, max length: 10`, or `Keep DECIMAL(5, 0) (already minimal precision)` / `Keep VARCHAR(3) UTF8 (n <= 3, left untouched)` |
| `query_text` | the generated `ALTER TABLE ... MODIFY ...` statement (and the FK DROP/RE-ADD statements and section dividers, see below) |
| `success` | only present when `apply_conversion = true`: `true`, the error message of the failing statement, `rolled back (a later statement failed)` or `not executed (stopped at the first error)` |

With `log_for_all_columns = true` every inspected column of an enabled type is listed — also the
**already-minimal** (`DECIMAL` precision ≤ 9, `VARCHAR` size ≤ 3) and empty columns, each with a `Keep …` row.
Without `reduce_decimal_scale`, a kept `DECIMAL(p,s)` row whose precision could not be reduced carries the hint
that a lossless scale reduction can be checked.

If the result would be empty, the script returns a single informative row in the `conversion`
column instead:
- `log_for_all_columns = false` → `No columns found that need optimization.`
- `log_for_all_columns = true`  → `No matching columns found (check the filters and the convert_* switches).`
  — i.e. no column matched the schema/table filter and the enabled `convert_*` switches at all.

### Applying (`apply_conversion = true`): all-or-nothing

The script first **commits** any pending work of your session, then runs the analysis **and** all statements
in **one transaction**, in the shown order (DROP FOREIGN KEYS → column changes → scale reductions → RE-ADD
FOREIGN KEYS). At the **first error** it **rolls back everything** it executed in this run — including
dropped foreign keys — and stops: the `success` column shows the error on the failing row, `rolled back` on
the earlier rows and `not executed` on the later ones. A fully successful run is **committed** at the end
(also when your client runs without autocommit). Your tables are therefore never left half converted or
without their foreign keys.

Do not run an apply while the analysed tables are being loaded. A concurrent change that reaches a table before
the script alters it ends in a *transaction collision*, which is reported as an error and rolls the whole run
back — no value is ever converted without having been checked. A write that arrives after the script has
altered a table waits for the script's `COMMIT` and is then stored with the **new** type (more decimals are
rounded, like in any later load — see the warning above).

The apply needs `ALTER` on every table it changes (and `REFERENCES` for re-added foreign keys). Missing
privileges are not checked in advance: the statement fails and the whole run is rolled back.

### Foreign keys (handled automatically)

A type change on a **primary/foreign key** column fails in Exasol unless the linked PK and FK columns are
changed to the **same** type — identical precision **and** scale (`constraint violation … wrong types`). When
`FOREIGN KEY`s touch the analyzed tables, the script handles this:

- **DROP/RE-ADD wrapper:** the output gains a **`-- ### DROP FOREIGN KEYS - run these FIRST (before the column changes) ###`**
  step and a **`-- ### RE-ADD FOREIGN KEYS - run these LAST (after the column changes) ###`** step (in the `query_text` column; composite FKs included), so
  the script — and `apply_conversion = true` — runs end to end. Each FK is re-added in its **original
  `ENABLE`/`DISABLE` state** (never changed).
- **Type harmonization:** every referential key group is converted to **one common target type** that fits all
  its columns (never a blanket VARCHAR) — with `reduce_decimal_scale` also one common scale; if no common
  smaller type exists, the whole group is kept (FK stays valid): a member kept for its own reason shows that
  reason, the other members name it (e.g. the member whose `DEFAULT` supplies a time of day). A group whose key
  columns are **all empty** is kept as well (an empty column says nothing about the values that will be loaded later).
- **Single table with an FK to another table:** if a key column's group reaches a table **outside the filter**
  (or one the user cannot see), the column is kept unchanged. When the columns in the filter could be converted
  (or are empty), a note names the other table(s) to include in a re-run — a key group is only checked and
  converted as a whole; this note is **always** shown, also with `log_for_all_columns = false`.
- **Foreign keys are read from `EXA_DBA_CONSTRAINTS`** when the user may read it (e.g. with `SELECT ANY
  DICTIONARY`), so that foreign keys on tables the user cannot see are known as well; otherwise from
  `EXA_ALL_CONSTRAINTS` — then the user must be able to see every referencing table (if not, the apply fails at
  that key column and is rolled back completely).
- With **no** foreign keys in scope the run is exactly as before (one cheap catalog check is the only overhead).
- If the foreign key catalog cannot be read at all (e.g. the session's `QUERY_TIMEOUT` is reached), the script
  **stops with an error** instead of proposing key column changes without the foreign key handling.

### How it works

- **One statistics scan per column.** Each column is measured with one aggregate query (not-null count plus
  the relevant length / digit / scale information). `DOUBLE` columns usually get one or two additional
  verification scans against their target type (members of a `DOUBLE` foreign key group one more each).
- **Robust.** A column whose check fails (e.g. a table the user may see but not read) is kept with
  `Keep … (check failed: <error>)` (listed also with `log_for_all_columns = false`); the rest of the run continues. All system objects are referenced with their
  schema (`SYS.EXA_ALL_COLUMNS`, `SYS.DUAL`, …), so objects of the same name in the current schema cannot interfere.
- **Exact scale measurement.** The decimals a `DECIMAL` column really uses are measured with
  `TRUNC()` in exact DECIMAL arithmetic; a scale reduction is only proposed when it is lossless.
- **DOUBLE checked against the real target.** The integer test is `v = FLOOR(v)` (exact in DOUBLE
  arithmetic), and every DOUBLE target is verified with a round trip through its real target type
  (`DECIMAL(9,s)` first, then `DECIMAL(18,s)`), i.e. always against exactly the type the `ALTER … MODIFY` will
  produce.
- **Parameter-independent timestamp check.** The `TIMESTAMP -> DATE` detection uses
  `TRUNC()` and therefore does not depend on the `TIMESTAMP_ARITHMETIC_BEHAVIOR`
  database parameter. It covers both `TIMESTAMP` and `TIMESTAMP WITH LOCAL TIME ZONE`
  of any precision `p` (0–9); for the time-zone variant the check and conversion are
  evaluated in the current `SESSIONTIMEZONE`.

### Case sensitivity

Exasol folds **unquoted** identifiers to UPPER CASE, while **delimited** (double-quoted)
identifiers are case-sensitive (default `SQL_IDENTIFIER_COMPARISON = CASE SENSITIVE`).
The script processes every schema/table/column name as a delimited identifier (via
`quote()` and the `::identifier` placeholder), so `MixedCase` and `lowercase` names — and names
containing double quotes — are handled correctly.

The `schema_name` / `table_name` **filter**, however, is a value comparison against the
names as stored in the catalog. Pass the name exactly as it was created (e.g.
`'MixedCase'`, not `'MIXEDCASE'`), or use a pattern with `%`. The pattern is evaluated with an explicit escape
character (backslash), independent of the session parameter `DEFAULT_LIKE_ESCAPE_CHARACTER`.

### Recommended workflow

1. Run with `apply_conversion = false` and **carefully review every proposed `ALTER` statement** —
   especially the `SCALE REDUCTIONS` section when `reduce_decimal_scale = true`.
2. Then choose one:
   - **✅ Safest (recommended):** keep `apply_conversion = false`, copy the statements from the
     `query_text` column, and run them **yourself, one statement at a time**, checking each result.
   - 🛑 Or set `apply_conversion = true` — **only if you are 100% sure**; this applies *all* changes
     irreversibly in one go (all-or-nothing: at the first error everything is rolled back).

## Migrate primary keys <a name="migrate_primary_keys"></a>

See script [set_primary_keys.sql](set_primary_keys.sql)
