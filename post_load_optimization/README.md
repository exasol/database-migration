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

The `convert_varchar.sql` script inspects the **`VARCHAR` columns** that match a schema/table filter and
suggests the smallest fitting data type for each column. It is meant for the typical post-import situation
where everything arrived as `VARCHAR`. The values are classified on a **random sample** (or on every row),
and every proposed conversion is then **verified against the full column**, so a suggestion never rounds,
truncates or rejects a value of the table.

> 📖 **Background:** see Exasol's [Performance Best Practices](https://docs.exasol.com/db/latest/performance/best_practices.htm) —
> using the smallest sufficient data type improves compression and join/query performance.

> 🔁 **Recommended order: run `convert_varchar` FIRST, then `convert_datatypes`.**
> `convert_varchar` turns string columns into their *true* types (`VARCHAR` → `DECIMAL`/`DATE`/
> `TIMESTAMP`/`BOOLEAN`/…). Afterwards run [`convert_datatypes`](#optimize_datatypes) to squeeze those
> (and the existing numeric/timestamp/varchar columns) down to the smallest sufficient size. Doing it the
> other way round misses the columns that are still `VARCHAR`.

Only **`VARCHAR` columns of real, local base tables** are inspected (`COLUMN_OBJECT_TYPE = 'TABLE'` and
`COLUMN_IS_VIRTUAL = FALSE`). **`CHAR` columns are not analysed**, and neither are views, synonyms or
virtual schema columns.

> ### ⚠️ REPORT ONLY — and review before you apply anything
> This script **does not change your data**. It only **returns rows** that describe the suggestion and the
> statement(s) you *could* run (in the `query_text` column). You review them and run them yourself.
>
> A conversion can be **LOSSY** even when every value fits. The `notes` column flags these cases per column:
> - `'007'` → `7`, `'+49'` → `49`: leading zeros and signs are lost (`WARNING`). Zip codes, phone numbers and
>   article numbers are typical traps.
> - `'1.10'` → `1.1`: the text form of decimals with trailing zeros (e.g. version numbers) is lost (`NOTE`).
> - `' 5 '` → `5`, `'5.'` → `5`, `'.5'` → `0.5`, `'-0'` → `0`: blank padding (also of dates and timestamps) and
>   these number forms are lost (`NOTE` with the number of values).
> - `'Y'`, `'yes'`, `'T'`, `'1'` all become `TRUE`: different boolean texts are merged (`NOTE`).
> - `DOUBLE PRECISION` keeps about 15 significant digits and drops the text form (`WARNING`, see below).
> - Integers that look like dates (`YYYYMMDD`) stay numbers (the note suggests `TO_DATE(col, 'YYYYMMDD')`).
> - Values loaded **later** must fit the new type as well: a `DECIMAL(p,s)` rounds extra decimals silently,
>   a `DATE` column rejects text that does not parse.

### How the decision is made

1. **Random sample.** For each table the values are classified on a random sample
   (`WHERE RANDOM() < p`, capped at the requested number of rows), or on the whole table for `'100%'`,
   whenever the requested sample is at least as large as the table, and for **every table with at most
   500,000 rows when the requested sample covers at least 5% of it** (see *Automatic full scan* below). A
   random sample finds rare values far better than a block of rows, but it is not reproducible and it can
   never prove that *every* value fits.
2. **Full-table verification.** Every conversion proposed from a sample is checked against the **full
   column** (plus the column `DEFAULT`) with the same classification. The decisive facts are measured over
   the full column: integer digits, decimals, fractional-second digits, date or timestamp, two-digit years,
   day/month ambiguity and the explicit date format. Size facts always come from the full column, so nothing
   is rounded or truncated.
3. **Misfit = decision on the full column.** If even one value of the full column does not fit the
   proposal, the column is classified again over the full column (exactly as with `'100%'`), and a note says
   how many values did not fit the sampled proposal (e.g. `The sample suggested DECIMAL, but 3 of 50000
   values in the full column do not fit; …`).

The suggested conversion (`conversion` and `query_text`) therefore does **not** depend on the random draw:
a sampled run gives the same suggestion as a `'100%'` run. Only these informational notes describe the
random draw, so they can appear in one run and not in the next:
- `Values read: …` (the table was sampled, or read completely, and why);
- `The sample suggested …, but N of M values in the full column do not fit; the column was therefore
  classified over the full column.`;
- `The sample matched the format '…', but k of N values in the full column do not match it (checked against
  the full column).`;
- counts `… in the sample …`, e.g. `N values in the sample look like timestamps with a time zone …`;
- `The random sample contained no value of this column, so the full column was classified.`

The width of a `VARCHAR` shrink and the "no data" check always use the full column.

**Automatic full scan.** Because every sampled proposal is verified against the full column anyway, a small
table is often cheaper to read completely than to sample and verify. A table with at most **500,000 rows** is
therefore read completely, even when a sample is requested, **if the requested sample covers at least 5% of
the table**: a percentage of at least `'5%'`, or a number of rows of at least 5% of the table's row count (so
for a number of rows *n* the limit is the smaller of 500,000 and 20 × *n*). A smaller sample keeps the sampled
path. The result is the same, and the rows of such a table note `Values read: the whole table (N rows; tables
up to L rows are read completely when the requested sample covers at least 5% of them - on the reference
cluster a full scan was not slower than a 5% sample at 250,000 and 500,000 rows; the suggestion is the same).`

The rule comes from one run per point on a 4-node cluster with a 60-column table (time of the full scan minus
the time of the sample; negative = the full scan is faster):

| Table rows | `'1%'` | 2% | `'3%'` | 4% | `'5%'` |
|---|---|---|---|---|---|
| 250,000 | +1.13 s | +0.52 s | -0.27 s | -0.10 s (10,000 rows) | -0.61 s |
| 500,000 | +2.38 s | +1.72 s (10,000 rows) | +0.82 s | -0.02 s | -0.62 s |
| 750,000 | +3.30 s | – | – | – | +0.67 s |
| 1,000,000 | +4.55 s | – | – | – | +1.53 s |

`'5%'` and `'100%'` cross at about 620,000 rows (straight line between 500,000 and 750,000); the limit 500,000
stays below that, and at 5% the full scan was faster at both measured sizes up to the limit. Small samples
(`'1%'`, or 10,000 rows of a 500,000-row table) were materially faster than a full scan, so a sample below 5%
is not replaced by one (between 3% and 4% the two were about equal). These numbers were measured on one
4-node cluster; the crossover depends on the hardware, the number of columns and the data. The limit is the
constant `full_scan_max_rows` in the `OPT` line of the script (`0` = always sample as requested).

### NLS settings: self-contained statements

Values are classified with the **session NLS settings at analysis time** (`NLS_DATE_FORMAT`,
`NLS_TIMESTAMP_FORMAT`, `NLS_NUMERIC_CHARACTERS`; `NLS_DATE_LANGUAGE` for month and day names). The decimal
separator is taken from `NLS_NUMERIC_CHARACTERS`, so German `1234,56` is a number under `',.'`.

Because a `MODIFY COLUMN` from `VARCHAR` parses the values with the NLS settings of the session that runs
it, **every NLS-dependent statement is self-contained**:

- A `DATE`, `TIMESTAMP`, `DOUBLE PRECISION` or `DECIMAL` (with decimals) cell starts with the
  `ALTER SESSION SET NLS_DATE_FORMAT` / `NLS_TIMESTAMP_FORMAT` / `NLS_NUMERIC_CHARACTERS` it needs, exactly as
  it was during the analysis (or as the detected explicit format requires). For a format with month or day
  names, `NLS_DATE_LANGUAGE` is set as well. Integer-only `DECIMAL(p,0)` cells need no prefix.
- The **first output row** (header row) states the NLS settings of the analysis and how the values were read
  (random sample and its size, or full scan).
- The **last output row** restores the session NLS settings that were active when the analysis started
  (only when a statement changes them) and ends with `COMMIT;`. Run it last. In Exasol `ROLLBACK` also
  reverts `ALTER SESSION`; the `COMMIT` keeps a later `ROLLBACK` in the same session from putting the session
  back on the NLS of a cell (e.g. an explicit `'MM.DD.YYYY'`). After a `ROLLBACK`, run the restore row again.

You can therefore run the cells in any session, and the column cells in any order.

### Multi-format detection (explicit format models)

For columns that the session formats do not classify, the script probes a set of common explicit formats and
uses one only when **exactly one** of them parses **every** value (the full column decides, also in a sampled
run). The cell then sets that format itself:

| Example values | Output |
|---|---|
| `12.06.2026`, `31.12.2025` | `ALTER SESSION SET NLS_DATE_FORMAT = 'DD.MM.YYYY';` + `… MODIFY COLUMN … DATE;` |
| `06/15/2026` | `… NLS_DATE_FORMAT = 'MM/DD/YYYY';` + `… DATE;` |
| `2026.06.12` | `… NLS_DATE_FORMAT = 'YYYY.MM.DD';` + `… DATE;` |
| `12.06.2026 10:00:00.123456` | `… NLS_TIMESTAMP_FORMAT = 'DD.MM.YYYY HH24:MI:SS.FF6';` + `… TIMESTAMP(6);` |
| `2026-06-12T10:00:00.5` (ISO 8601) | `… NLS_TIMESTAMP_FORMAT = 'YYYY-MM-DDTHH24:MI:SS.FF1';` + `… TIMESTAMP(1);` |
| `01.02.2026`, `03.04.2026` (every day ≤ 12) | **Keep** (ambiguous date format): DD.MM and MM.DD both fit, so the order is not guessed |
| `12.06.26` | **Keep** (two-digit years): the century is not guessed; the note names the two-digit forms the values fit (`'YY.MM.DD', 'DD.MM.YY'`) |

Probed formats: `YYYY-MM-DD`, `YYYY.MM.DD`, `YYYY/MM/DD`, `DD.MM.YYYY`, `MM.DD.YYYY`, `DD/MM/YYYY`,
`MM/DD/YYYY`, `DD-MM-YYYY`, `MM-DD-YYYY`, each also followed by ` HH24:MI:SS[.FFn]`, and the ISO 8601 form
`YYYY-MM-DDTHH24:MI:SS[.FFn]`. The fractional-second precision (0–9) is measured after the seconds. Any two
matching formats count as ambiguous, not only a day/month swap. The probe runs only for unclassified columns
whose values look date-like and whose longest value has 6 to 40 characters. Times must be 24-hour
`HH24:MI:SS[.FFn]` (missing minutes or seconds are read as 0); other time formats (e.g. 12-hour `AM` / `PM`) are
not probed (see *Date vs. timestamp*). A value with a time part is only checked against the `TIMESTAMP` formats,
so it is never read as a `DATE`. When every time of the column is midnight (`'12.06.2026 00:00:00'`, also mixed
with plain dates), the cell reads the values with that `TIMESTAMP` format and then changes the column to `DATE`.

### What it detects

| If the values look like … | Suggestion |
|---|---|
| integers | `DECIMAL(p, 0)`, precision rounded up to 9 / 18 / 36 (leading zeros are not counted) |
| integers and decimals (`'5'`, `'-1.25'`, `'.5'`, `'5.'`) | `DECIMAL(p, s)`, precision rounded up to 9 / 18 / 36, scale **s** = the most decimals of any value |
| numbers incl. scientific notation (`'1E3'`) | `DOUBLE PRECISION` with a lossy `WARNING`, only within the limits below |
| dates only (no time part) | `DATE` |
| dates and/or timestamps with at least one time of day other than midnight | `TIMESTAMP(p)` with the measured fractional-second precision `p` (0–9), plus a hint to consider `TIMESTAMP WITH LOCAL TIME ZONE`; a value with a time part is never read as a `DATE` |
| dates and/or timestamps whose times are **all** midnight (`'2000-01-02 00:00:00'`, `'… 00:00:00.000'`, also mixed with plain dates; the `DEFAULT` included) | `DATE`: the cell reads every value with the exact `TIMESTAMP` format (`MODIFY COLUMN … TIMESTAMP(p)`) and then changes the column to `DATE`, which drops only the time `00:00:00` (see *Date vs. timestamp*) |
| boolean texts | `BOOLEAN` (see below) |
| only `0` / `1` | `BOOLEAN`, with a `NOTE` to verify that these are real booleans (not flags, bits or codes); a FOREIGN KEY key column is converted as a number (see *Foreign keys*) |
| day-to-second intervals (`'5 10:00:00.5'`) | `INTERVAL DAY(p) TO SECOND(fp)` (fp ≤ 3) |
| year-to-month intervals (`'5-01'`, `'-123-01'`) | `INTERVAL YEAR(p) TO MONTH` (the sign is not counted as a digit) |
| WKT geometry (`POINT (…)`, `POLYGON (…)`, …, `… EMPTY`) | `GEOMETRY`, plus a hint to specify an SRID |
| no single type fits, but the values are shorter than the column | a smaller `VARCHAR` with the **character set kept** (see *How it works*); columns with n ≤ 3 are not shrunk |
| the **column name** looks like a date/timestamp, but the values do not parse | a hint with an example `UPDATE` / `ALTER`, next to the normal shrink suggestion |
| the column has no value at all (full column checked) | `Keep … (no data - candidate for DROP COLUMN)` |

**BOOLEAN texts.** Every text `IS_BOOLEAN` accepts is merged into `TRUE` / `FALSE`. On Exasol 2025.1.16 these
are `TRUE`/`FALSE`, `T`/`F`, `Y`/`N`, `YES`/`NO`, `1`/`0` and `01`/`00` (any case). The original texts are lost.
A PRIMARY KEY column whose boolean texts would give duplicate keys is kept (see *PRIMARY KEY columns*).

**PRIMARY KEY columns.** A PRIMARY KEY column (also a column of a composite PRIMARY KEY) gets a conversion only
when its keys stay different. Different texts can become the same value: `'Y'` and `'1'` both become `TRUE`;
`'1'`, `' 1'`, `'01'` and `'1.0'` are the same `DECIMAL`; `'2020-01-01'` and `' 2020-01-01'` are the same
`DATE`; `'… 10:00:00'` and `'… 10:00:00.0'` the same `TIMESTAMP`; `'2020-01-01'` and `'2020-01-01 00:00:00'`
the same `DATE` (all times midnight) or `TIMESTAMP`. The
distinct key values are counted over the full table with the texts and with the converted values (with the
same format / NLS settings as the generated statement). When they collide, the column is kept
(`Keep … (PRIMARY KEY: values would collide as DECIMAL(9, 1) - not changed)`, with both counts): the `MODIFY`
would fail with a primary key constraint violation, or leave duplicate keys when the PRIMARY KEY is disabled.
The note gives an example for each target type of the key (DECIMAL, DOUBLE, DATE, TIMESTAMP, intervals,
BOOLEAN); for a composite key it names the type of each converted member with its column, e.g.
`As DECIMAL(9, 1) (column "A") and DATE (column "B") …`, and each member's `Keep` reason names its own type.
A FOREIGN KEY group with such a member is kept as a whole.

**Column-name hints.** A name gets a date or timestamp hint only when one of its `_`-separated parts is a
whole token `TS`, `TIMESTAMP`, `DATETIME`, `DT`, `DATE` or `DOB` (e.g. `ORDER_DATE`, `CREATED_TS`, but not
`UPDATED`, `PRODUCTS` or `ADDRESS_DTL`). The hint uses `TIMESTAMP(p)` and an NLS-independent recipe.

### What is kept, and why

These columns are **not** converted. The `conversion` column reads `Keep …` with the reason, and the note
explains it:

| Case | Why |
|---|---|
| Values do not fit one type | No conversion fits every value; a smaller `VARCHAR` is suggested when it saves space. |
| `DOUBLE PRECISION` candidates with more than 15 significant digits, a leading zero, a `+` sign, or a value below the `DOUBLE` range | `DOUBLE` would lose digits, the zero or the sign, or turn the value into 0. |
| Numbers that need more than 36 digits | `DECIMAL` holds at most 36 digits. |
| Calendar months `YYYY-MM` (`'2024-01'`) | They are months, not durations: `INTERVAL YEAR TO MONTH` would read `2024` years. The note suggests `TO_DATE(col, 'YYYY-MM')` into a new `DATE` column. |
| Two-digit years (`'12.06.26'`), in the explicit formats **and** in the session formats | `YYYY` would read `26` as year 0026, `YY` / `RR` pick a century implicitly. Decide the century yourself. |
| Ambiguous day/month order (every day ≤ 12, or more than one explicit format fits) | The order cannot be told from the data. Pick it yourself. |
| Timestamps with a time zone (`'…Z'`, `'…+02:00'`) | A `TIMESTAMP` conversion would drop the zone. Normalize them to one zone first. |
| Dates mixed with timestamps when a date does not parse with the target `TIMESTAMP` format | The `MODIFY` would fail. Bring the values into one format first. |
| `INTERVAL DAY TO SECOND` values with more than 3 fractional-second digits | The type keeps milliseconds only; the other digits would be cut off. |
| `TIMESTAMP` / interval precision above 9 | The maximum is 9. |
| A column `DEFAULT` that is not a quoted text or an integer: a non-literal `DEFAULT` (e.g. `CURRENT_USER`, reported as `non-literal DEFAULT …`) or an unquoted decimal number (e.g. `DEFAULT 1.5`, reported as `unquoted decimal number DEFAULT 1.5`) | It cannot be checked against a new type. |
| A PRIMARY KEY column whose texts would give duplicate keys after the conversion (`'Y'` / `'1'` as `BOOLEAN`, `'1'` / `'1.0'` / `' 1'` / `'01'` as `DECIMAL`, a padded date, a date with and without `00:00:00` as `DATE` or `TIMESTAMP`) | The `MODIFY` would fail with a primary key constraint violation, or leave duplicate keys under a disabled PRIMARY KEY (see *PRIMARY KEY columns*). |
| Key columns of a FOREIGN KEY group without a common type, or whose group reaches outside the filter | See *Foreign keys*. |

### Parameters

```sql
execute script DATABASE_MIGRATION.CONVERT_VARCHAR(
    'MY_SCHEMA',   -- schema_pattern:      schema name or LIKE pattern (case-exact)
    '%',           -- table_pattern:       table name or LIKE pattern (case-exact)
    '5%',          -- sample_size:         rows per table (min 1000) or a percentage like '5%'; '100%' = every row
                   --                      (tables with at most 500,000 rows are read completely when
                   --                      the sample covers at least 5% of them)
    false          -- log_for_all_columns: false = changes + errors + FOREIGN KEY notes, true = every inspected column
);
```

| Parameter | Description |
|-----------|-------------|
| `SCHEMA_PATTERN` | Schema name or `LIKE` pattern, non-empty. It is **always** a `LIKE` pattern: `%` matches any characters and `_` matches any single character, also in a name without `%` (`'MY_SCHEMA'` also matches `MYXSCHEMA`). A backslash escapes a following `%`, `_` or backslash (`'MY\_SCHEMA'` matches only `MY_SCHEMA`); any other backslash, also a trailing one, is an ordinary character. The escape character is always the backslash, independent of the session parameter `DEFAULT_LIKE_ESCAPE_CHARACTER`. The comparison is **case-exact** against the names as stored in the catalog (pass `'MixedCase'`, not `'MIXEDCASE'`). |
| `TABLE_PATTERN` | Table name or `LIKE` pattern, same rules as `SCHEMA_PATTERN`. |
| `SAMPLE_SIZE` | Rows per table: an integer (values below 1000 are raised to 1000), **or** a percentage string greater than 0% and at most 100% (`'5%'`, `' 50 %'`, `'2.5%'`); a percentage also reads at least 1000 rows per table. `'100%'` reads every row without sampling, and so does any sample that is at least as large as the table and any table with at most 500,000 rows when the sample covers at least 5% of it (*Automatic full scan*). Any other value (e.g. `'0%'`, `'150%'`, `'abc'`, `NULL`) gives the error row `Invalid sample_size: …` and no suggestion. Every proposal is verified against the full column in any case; the sample size only decides how many rows the first classification reads. |
| `LOG_FOR_ALL_COLUMNS` | `false` = report the columns that get a statement, plus every `Could not analyze …` row and every FOREIGN KEY note (key groups that are blocked from a conversion, key columns whose group reaches outside the filter, `Not changed …` rows when the FOREIGN KEY catalog cannot be read). `true` = report **every** inspected column, including `Keep …` rows with the reason, empty tables and empty columns. Only the boolean `true` switches it on; `NULL` and the string `'false'` count as `false`. |

### Output

All six output columns are **`VARCHAR(2000000)`**, so long statements and notes are never cut off or padded.

| Column | Meaning |
|--------|---------|
| `schema_name`, `table_name`, `column_name` | the inspected column (empty for header, note, divider, COMMIT and restore rows) |
| `conversion` | short description, e.g. `VARCHAR(50) UTF8 --> DECIMAL(9, 0)`, `VARCHAR(20) UTF8 --> DATE (format DD.MM.YYYY)` or `Keep VARCHAR(100) UTF8, max length: 12` |
| `query_text` | the statement(s) to run, including their `ALTER SESSION` prefix and, for a literal `DEFAULT`, the `SET DEFAULT` statement; empty for `Keep` rows; an SQL comment (`-- ### … ###`) for divider rows |
| `notes` | warnings, notes, hints and recipes for this column |

Rows, in this order:
1. **Header row**: `Analysis settings: NLS_DATE_FORMAT = '…', NLS_TIMESTAMP_FORMAT = '…', NLS_NUMERIC_CHARACTERS = '…'; values read: …`
   (`random sample of about …; … tables with at most 500000 rows are always read completely (the requested
   sample covers at least 5% of each of them; on the reference cluster a full scan was not slower than a 5%
   sample at 250,000 and 500,000 rows); …` - the rule part only when the sample covers at least 5%; for a number
   of rows *n* the limit shown is the smaller of 500,000 and 20 × *n*; the rule part is left out when that limit is
   not larger than *n* (then `tables with at most n rows are read completely` already covers it). The number of
   rows is printed as an exact integer; a `sample_size` of 10^15 rows or more is shown as `sample_size of at least
   1000000000000000 rows: every table with fewer rows is read completely (in practice a full scan)` - or `full scan - no sample: every row
   of every table (100%)`), with the
   `query_text` comment `-- ### convert_varchar report (REPORT ONLY) - review every statement before running it ###`
   and the transaction guidance in `notes`.
2. **Note rows**, only when relevant: `NOTE: FOREIGN KEYs were read from SYS.EXA_ALL_CONSTRAINTS …`,
   `ERROR: the FOREIGN KEY catalog could not be read` or, after an internal error, `ERROR: the FOREIGN KEY
   handling failed` (then no statement is given for any column) / `ERROR: the PRIMARY KEY check of the proposals
   failed` (then no column gets a proposal that changes the values: `BOOLEAN`, `DECIMAL`, `DOUBLE PRECISION`,
   `DATE`, `TIMESTAMP` or an interval).
3. **One row per column**, sorted by schema, table and column. When a FOREIGN KEY block follows, these rows
   come under the divider `-- ### COLUMN TYPE CHANGES (no FOREIGN KEY involved) ###`.
4. A **`COMMIT;` row** after the column changes (only when there is at least one).
5. The **FOREIGN KEY notes**: key columns that are **not** changed (`Keep … (FK key column - related table out of
   scope)`, `Keep … (FK key group: no common convertible type - not changed)`), with the reason and without a
   statement, under the divider `-- ### FOREIGN KEY NOTES (not changed) ###` (only when there are such rows).
6. The **FOREIGN KEY block**, when key columns change (see *Foreign keys*).
7. The **NLS restore row** (only when a statement changes the NLS settings); it ends with `COMMIT;`.

Other row texts: `Table is empty (no data)`, `Could not analyze VARCHAR(…) …` (query or internal error, or a
table that cannot be read), `Keep … (needs N digits > max 36)`, `Keep … (FK key column - related table out of
scope)`, `Keep … (FK key group: no common convertible type - not changed)`, the tag `[FK key group -
harmonized]` on key column changes, and `Not changed: … (suggested type …) - the FOREIGN KEY catalog could not
be read`. If nothing matches the filter, the single row `No matching VARCHAR columns found (check the
schema/table filter; LIKE patterns are case-exact).` is returned; if columns match but none has to be shown,
`No columns found that need optimization.` In a sampled run, every analysed column row also notes whether its table
was sampled (`Values read: a random sample of about n of N rows; …`) or read completely (`Values read: the
whole table (N rows; the requested sample covers it).` or `… tables up to L rows are read completely when the
requested sample covers at least 5% of them - on the reference cluster a full scan was not slower than a 5%
sample at 250,000 and 500,000 rows; the suggestion is the same).`).

Example (`'100%'`, `log_for_all_columns = true`, session NLS at the ISO default):

| schema_name | table_name | column_name | conversion | query_text | notes |
|---|---|---|---|---|---|
| | | | `Analysis settings: NLS_DATE_FORMAT = 'YYYY-MM-DD', NLS_TIMESTAMP_FORMAT = 'YYYY-MM-DD HH24:MI:SS.FF6', NLS_NUMERIC_CHARACTERS = '.,'; values read: full scan - no sample: every row of every table (100%)` | `-- ### convert_varchar report (REPORT ONLY) - review every statement before running it ###` | Run each query_text cell as a whole: … Run with AUTOCOMMIT OFF: … |
| MY_SCHEMA | CUSTOMERS | ACTIVE | `VARCHAR(10) UTF8 --> BOOLEAN` | `ALTER TABLE "MY_SCHEMA"."CUSTOMERS" MODIFY COLUMN "ACTIVE" BOOLEAN;` | NOTE: only 0/1 values. Verify these are real booleans, not flags, bits or codes you compute with. |
| MY_SCHEMA | CUSTOMERS | AMOUNT | `VARCHAR(20) UTF8 --> DECIMAL(9, 2)` | `ALTER SESSION SET NLS_NUMERIC_CHARACTERS = '.,'; ALTER TABLE "MY_SCHEMA"."CUSTOMERS" MODIFY COLUMN "AMOUNT" DECIMAL(9, 2);` | |
| MY_SCHEMA | CUSTOMERS | COMMENT | `VARCHAR(2000000) UTF8 --> VARCHAR(20) UTF8, max length: 15` | `ALTER TABLE "MY_SCHEMA"."CUSTOMERS" MODIFY COLUMN "COMMENT" VARCHAR(20) UTF8;` | No single data type fits all values. Width reduced to the longest value (15 characters, …) + 20% headroom, rounded up; character set kept. |
| MY_SCHEMA | CUSTOMERS | CUST_ID | `VARCHAR(50) UTF8 --> DECIMAL(9, 0)` | `ALTER TABLE "MY_SCHEMA"."CUSTOMERS" MODIFY COLUMN "CUST_ID" DECIMAL(9, 0);` | WARNING: 3 values have leading zeros or a '+' sign (identifier-like: ID / ZIP / phone / article no.). DECIMAL LOSES them ('007' -> 7, '+49' -> 49). Review before applying! |
| MY_SCHEMA | CUSTOMERS | DE_DATE | `VARCHAR(20) UTF8 --> DATE (format DD.MM.YYYY)` | `ALTER SESSION SET NLS_DATE_FORMAT = 'DD.MM.YYYY'; ALTER TABLE "MY_SCHEMA"."CUSTOMERS" MODIFY COLUMN "DE_DATE" DATE;` | Values match the explicit format 'DD.MM.YYYY' (not the session format); query_text sets it with ALTER SESSION first. |
| MY_SCHEMA | CUSTOMERS | ORDER_DATE | `VARCHAR(20) UTF8 --> DATE` | `ALTER SESSION SET NLS_DATE_FORMAT = 'YYYY-MM-DD'; ALTER TABLE "MY_SCHEMA"."CUSTOMERS" MODIFY COLUMN "ORDER_DATE" DATE;` | |
| MY_SCHEMA | CUSTOMERS | PERIOD | `Keep VARCHAR(7) UTF8 (calendar months YYYY-MM - not converted to an interval)` | | Values such as '2024-01' are calendar months, not durations; … use TO_DATE("PERIOD", 'YYYY-MM') in a new DATE column. |
| | | | `end of the column type changes - COMMIT only when every statement above succeeded …` | `COMMIT;` | |
| | | | `Restore the session NLS settings that were active when the analysis started (run this last; it ends with COMMIT because in Exasol ROLLBACK also reverts ALTER SESSION - after a ROLLBACK run this row again)` | `ALTER SESSION SET NLS_DATE_FORMAT = 'YYYY-MM-DD'; ALTER SESSION SET NLS_TIMESTAMP_FORMAT = 'YYYY-MM-DD HH24:MI:SS.FF6'; ALTER SESSION SET NLS_NUMERIC_CHARACTERS = '.,'; ALTER SESSION SET NLS_DATE_LANGUAGE = 'ENG'; COMMIT;` | |

### Running the statements

- Run each `query_text` cell **as a whole** (its `ALTER SESSION` prefix belongs to it). The column cells do
  not depend on each other's order; only the FOREIGN KEY block has a fixed order, and the `COMMIT;` rows end
  a transaction. Run the restore row last.
- Run with **`AUTOCOMMIT OFF`**. The column changes and the FOREIGN KEY block are **two separate
  transactions**, each ended by its `COMMIT;` row. After any error run **`ROLLBACK`**: it reverts only the
  statements since the last `COMMIT` — inside the FOREIGN KEY block it also restores the dropped FOREIGN KEYs
  and the key columns, and the column changes committed before the block stay as they are.
- In Exasol **`ROLLBACK` also reverts `ALTER SESSION`**: after a `ROLLBACK` the session is back on the NLS
  settings of the last cell before the last `COMMIT` (e.g. an explicit `'MM.DD.YYYY'`), and your own date
  strings would then be read in that format. After a `ROLLBACK`, therefore run the **NLS restore row** (the
  last row) again. It ends with `COMMIT;`, so a later `ROLLBACK` cannot undo it. The header row and the
  FOREIGN KEY block guidance say so as well.
- A column `DEFAULT` is treated like one more value. A **literal** `DEFAULT` of a column that becomes `DATE`,
  `TIMESTAMP`, `DECIMAL` or `DOUBLE PRECISION` is re-set as a typed literal after the change
  (`ALTER TABLE … ALTER COLUMN … SET DEFAULT DATE '…'`), so new rows do not depend on the NLS settings of the
  inserting session. Only a quoted text or an integer counts as a literal `DEFAULT`; a column with any other
  `DEFAULT` (e.g. `CURRENT_USER`, or an unquoted decimal number such as `1.5`) is kept.

### Foreign keys (handled automatically)

In Exasol a type change on a **primary/foreign key** column fails unless the linked PK and FK columns are
changed to the **same** type (`constraint violation … wrong types`). When `FOREIGN KEY`s touch the analyzed
tables, the script handles this for you:

- **Type harmonization:** every referential key group (a PK column plus all FK columns linked to it,
  transitively) whose tables are all inside the filter gets **one common target type that fits all of its
  columns** (e.g. a 9-digit and a 12-digit key column → `DECIMAL(18, 0)`), never a blanket `VARCHAR`. A key
  group is **never** harmonized to `DOUBLE PRECISION` (approximate) or to more than 36 digits, and a `DATE`
  member of a `TIMESTAMP` group is verified with the common `TIMESTAMP` format. If the group has no common
  convertible type, all its columns are kept (`Keep … (FK key group: no common convertible type - not
  changed)`) and the FOREIGN KEY stays valid. A group whose columns are all kept for their own reasons is not
  reported as blocked.
- **Key columns without data:** a key column whose table is empty, or that is `NULL` in every row (and has no
  column `DEFAULT`), fits every type. It does not take part in finding the common type and gets the type the
  other members of its group agree on; its `MODIFY` is in the FOREIGN KEY block (note: `This key column holds
  no value …`). An empty or optional child table therefore no longer blocks its parent key. When **no** member
  of a group holds data, the group is kept as it is (`Table is empty (no data)` / `Keep … (no data …)`). A
  member without data but **with** a column `DEFAULT` is not treated this way: its `DEFAULT` is not checked
  against the common type, so the whole group is kept (note: `… it holds no data, but its column DEFAULT … is
  not checked against a common type`). Remove the `DEFAULT`, or convert the group yourself.
- **Key columns with only `0` / `1`:** on its own such a column becomes `BOOLEAN`; inside a key group it is
  merged as a **number**, so a group with a parent `'1'`, `'2'`,
  `'3'` and a child that holds only `'1'` becomes `DECIMAL(9, 0)` on both sides, and a key column is never
  `BOOLEAN` next to a numeric key column. Only next to a text boolean member (`Y`/`N`, `TRUE`/`FALSE`, …) does it
  stay `BOOLEAN`, and only when no PRIMARY KEY of the group would get duplicate keys (see *PRIMARY KEY
  columns*); otherwise the whole group is kept.
- **FOREIGN KEY block:** only the key columns of harmonized groups are changed inside the block; all other
  column changes are listed before it and stay outside. The block reads:
  ```
  -- ### FOREIGN KEY BLOCK - run the following rows as ONE transaction (AUTOCOMMIT OFF, ROLLBACK on any error) ###
  -- ### DROP FOREIGN KEYS - run these FIRST (before the key column changes) ###
  -- ### KEY COLUMN TYPE CHANGES ###
  -- ### RE-ADD FOREIGN KEYS - run these LAST (after the key column changes) ###
  COMMIT;
  ```
  Composite FOREIGN KEYs are included, and each FOREIGN KEY is re-added in its **original `ENABLE` /
  `DISABLE` state**. Key columns that are not changed are listed before the block, without a statement, under
  `-- ### FOREIGN KEY NOTES (not changed) ###`, never under the `no FOREIGN KEY involved` divider. When a
  statement in the block changes the NLS settings, the guidance row of the block also says to run the NLS
  restore row again after a `ROLLBACK`.
- **Group reaches outside the filter:** if a key column's group reaches a table **outside the filter**, the
  column is kept unchanged with a note that names the other table(s) (sorted): re-run with a
  `schema_pattern` / `table_pattern` that includes them, so the group is converted as a whole. This note is
  always shown, also with `log_for_all_columns = false`.
- **`EXA_DBA` / `EXA_ALL`:** FOREIGN KEYs are read from `SYS.EXA_DBA_CONSTRAINTS` when the user may read it
  (e.g. with `SELECT ANY DICTIONARY`), so FOREIGN KEYs on tables the user cannot see are known as well.
  Otherwise they are read from `SYS.EXA_ALL_CONSTRAINTS`, and a note row says that FOREIGN KEYs on invisible
  tables are unknown (a change of a primary key column that such a FOREIGN KEY references will fail).
- **Catalog not readable:** if the FOREIGN KEY catalog cannot be read at all (e.g. the session's
  `QUERY_TIMEOUT` is reached), an `ERROR` row is shown and **no statement** is given for any column, because
  the key columns are unknown; the rows still show the suggested type (`Not changed: …`).
- The FOREIGN KEY catalog is queried once per call.

### Privileges

- `SELECT` on the analyzed tables; a table that cannot be read gives `Could not analyze …` rows.
- Optional: `SELECT ANY DICTIONARY` (for `SYS.EXA_DBA_CONSTRAINTS`, see *Foreign keys*).
- To run the statements: `ALTER` on the tables, and `REFERENCES` for the re-added FOREIGN KEYs.

### How it works / notes

- **Cost.** Each table is counted once (`COUNT(*)`). Per column: one classification query (over the sample,
  or the whole table) and, when a sample is used, one verification query over the full column per proposed
  conversion; when the verification fails, one more classification over the full column. The full-column
  `COUNT` / `MAX(LENGTH)` of a sampled table is computed for up to 20 columns in one statement. A column that
  mixes dates and timestamps gets one more check of the target `TIMESTAMP` format. The multi-format probe
  runs only for unclassified columns whose values look date-like (on a sampled run once on the sample and,
  when that matches a format or its own sample holds no value, once on the full column). A table with a
  proposal that changes the values (`DECIMAL`, `DOUBLE PRECISION`, `DATE`, `TIMESTAMP`, intervals) is read once
  more for the blank-padding / number-form notes, and each PRIMARY KEY with such a column once more for the
  duplicate-key check (also with `'100%'`). A sampled run therefore reads each column more than once; the sample
  only shortens the first classification.
- **Faster evaluation, same result.** Every statement that reads at least 20,000 rows (a statement over the
  sample: the sample rows; a statement over the full column: the table rows) is evaluated in a faster way
  with identical facts: a few hint statements per table (distinct-value estimate, a text value that rules
  out every type), a classification of the DISTINCT values where there are few, a reduced query for text
  columns, checks skipped where their result is known (`GEOMETRY`, `IS_BOOLEAN`, date parsers on long texts)
  and a date probe over the distinct values (from 100,000 rows on, a format that fails on one of the first
  1000 distinct values is not checked further). Smaller statements run unoptimized (there the extra statements
  cost more than they save). Any optimized statement that fails is repeated in its plain form. The
  optimizations are switched in the `OPT` line of the script; with every flag `false` the script returns the
  same `conversion` and `query_text`.
- **Date vs. timestamp.** A `DATE` keeps no time of day, so the script decides by the time parts of the values
  and of the `DEFAULT`:
  - **No time part** (every value a plain date): `DATE`.
  - **Every time is midnight** (`00:00:00`, a fraction only of zeros, `'12:00:00 AM'` under a 12-hour session
    `NLS_TIMESTAMP_FORMAT`; plain dates may be mixed in): `DATE`, read with an **exact format that includes the
    time elements**, never with a lenient date parse. The cell sets that `TIMESTAMP` format, runs
    `MODIFY COLUMN … TIMESTAMP(p)` (every value read with that format; a plain date mixed in is checked against it
    over the full column first) and then `MODIFY COLUMN … DATE`, which drops only the time `00:00:00`. A literal
    `DEFAULT` is re-set as `DATE '…'` between the two (the change to `DATE` would otherwise convert the `DEFAULT`
    text with `NLS_DATE_FORMAT`). The conversion text says so (`--> DATE (read as TIMESTAMP(0) with format
    YYYY-MM-DD HH24:MI:SS.FF6; every time of day is 00:00:00)`).
  - **Any other time of day** (one value is enough; in a sampled run this is checked over the full column):
    `TIMESTAMP(p)` when `NLS_TIMESTAMP_FORMAT` or exactly one explicit format reads every value, otherwise the
    column stays text (smaller `VARCHAR`). The value is never read as a `DATE`.
  - **12-hour times** (`AM` / `PM`) that the session `NLS_TIMESTAMP_FORMAT` does not read stay text: the explicit
    formats are 24-hour only. Convert them yourself, e.g. into a new `TIMESTAMP` column with
    `TO_TIMESTAMP(col, 'MM/DD/YYYY HH12:MI:SS AM')`.

  A value has a time part when it has more digit groups than a date written in `NLS_DATE_FORMAT` (e.g.
  ` 10:00:01`, ` 00:00:00` or an hour alone). A FOREIGN KEY key group gets `DATE` by the midnight rule only when
  every member does; next to a plain `DATE` member it gets `TIMESTAMP(p)`. This matters because `IS_DATE` also
  accepts a trailing time in some formats: under `NLS_DATE_FORMAT = 'MM/DD/YYYY'` and `NLS_TIMESTAMP_FORMAT =
  'MM/DD/YYYY HH12:MI:SS AM'`, `'01/02/2000 10:00:01'` passes `IS_DATE` but not `IS_TIMESTAMP` (no AM/PM); it is
  converted with the explicit format `'MM/DD/YYYY HH24:MI:SS'` to `TIMESTAMP(0)`, never to `DATE`. A value without
  a time part is a timestamp only when `NLS_TIMESTAMP_FORMAT` gives it a time of day other than midnight
  (`TRUNC()`); the check does not depend on `TIMESTAMP_ARITHMETIC_BEHAVIOR` and works when `NLS_DATE_FORMAT`
  and `NLS_TIMESTAMP_FORMAT` differ. The same rule applies to the column `DEFAULT` and to the explicit formats
  (a value with a colon or more than three digit groups is only checked against the `TIMESTAMP` formats).
  When `NLS_DATE_FORMAT` itself contains time elements (e.g. `'MM/DD/YYYY HH12:MI:SS AM'` or
  `'DD.MM.YYYY HH24:MI'`), a date written in it has as many digit groups as a value with a time, and the
  midnight rule above is not used: a value is a `DATE` only when that format reads it at midnight
  (`'01/13/2000 12:00:00 AM'`, `'01/13/2000'`), other midnight timestamps keep `TIMESTAMP(p)`, and
  `'01/13/2000 10:00:01 PM'` (or a `DEFAULT` like it) is never converted to `DATE`: it gets a `TIMESTAMP` with an
  explicit format when one fits, otherwise the column stays text. The fractional-second precision is measured
  after the seconds (`.` or `,`), so the `.` of a `DD.MM.YYYY` date is never counted.

  Known limitations of the midnight rule (no value is lost in any of them):
  - A FOREIGN KEY key group that mixes plain dates in one member with midnight timestamps in another gets
    `TIMESTAMP(0)` for the whole group; a single column with the same mix gets `DATE`.
  - A column that mixes the ISO form with `T` and with a blank (`'2000-01-02T00:00:00'` and
    `'2000-01-02 00:00:00'`), or the `T` form with plain dates, matches no single format and only gets the
    `VARCHAR` shrink.
  - For an all-midnight column the `query_text` modifies the column twice (to `TIMESTAMP(p)` with the exact
    format, then to `DATE`), so applying it rewrites the column twice.
- **TIMESTAMP precision (0–9).** `TIMESTAMP(p)` uses the measured number of fractional-second digits. When the
  values have more digits than the session `NLS_TIMESTAMP_FORMAT` parses (e.g. 9 digits under `FF6`), the
  cell's own prefix sets the format with `FF<p>`, so no digit is lost. `TIMESTAMP WITH LOCAL TIME ZONE` is
  only a hint (it cannot be told apart from a plain timestamp by the text).
- **VARCHAR shrink width.** The longest value of the full column (incl. the `DEFAULT`) + 20%, rounded **up**
  to the next multiple of its leading power of ten (1 → 3, 8 → 20, 12 → 20, 500 → 700, 1000 → 2000), at most
  2,000,000. The character set is kept (`VARCHAR(2000000) ASCII` → `VARCHAR(20) ASCII`, never `UTF8`).
  Columns with n ≤ 3 are not shrunk.
- **Robust.** Each column is analyzed in a protected call: a failing query or an internal error yields one
  `Could not analyze …` row for that column, and the rest of the run continues. All system objects are
  referenced with their schema (`SYS.EXA_ALL_COLUMNS`, `SYS.EXA_PARAMETERS`, `SYS.DUAL`, …), so objects of the
  same name in the current schema cannot interfere. All identifiers are quoted, so mixed-case names and names
  with double quotes work. The time per value grows linearly with its length, also for values of up to
  2,000,000 characters (e.g. long runs of blanks): every text pattern is anchored at the first character, so it
  is not tried again at every position of the value.
- **Limitations.** Geometry is recognized by an anchored WKT text pattern (a heuristic). Numeric and date
  suggestions are lossy for identifier-like data (see the warning above). Values without seconds such as
  `'2026-06-12 10:00'`, or with an hour alone, are read with the missing parts as 0 by the session and the
  explicit `TIMESTAMP` formats (they become `TIMESTAMP(0)`; when every time is midnight, `DATE` by the midnight
  rule of *Date vs. timestamp*, read with the exact `TIMESTAMP` format).

### Performance

Measured on a 4-node Exasol cluster (version 2026.1.2) with tables of 60 `VARCHAR` columns of mixed content
(numbers, dates, timestamps, booleans, intervals, geometry, text), one run per scenario, query cache off. The times
depend on the hardware, the number of columns and the data, so treat them as an indication only.

| Table rows | `sample_size` | previous version | this version | this version read |
|---|---|---:|---:|---|
| 1,000,000 | `'100%'` | 90.58 s | 14.12 s | whole table (requested) |
| 1,000,000 | `'5%'` | 6.93 s | 11.99 s | random sample, verified against the full column |
| 1,000,000 | `'1%'` | 3.32 s | 9.46 s | random sample, verified against the full column |
| 10,000,000 | `'100%'` | 879.13 s | 75.63 s | whole table (requested) |
| 10,000,000 | `'5%'` | 49.67 s | 46.94 s | random sample, verified against the full column |
| 10,000,000 | `'1%'` | 11.84 s | 38.27 s | random sample, verified against the full column |

The times of this version were measured with this script, except 10,000,000 rows with `'1%'`, which was measured
with the version just before the last change of the date / timestamp rules (that change moved the re-measured times
by at most 5%).

- **Full scans** (`'100%'`) are about 6 to 12 times faster than with the previous version (6.4x on 1,000,000 and
  11.6x on 10,000,000 rows).
- **Sampled runs** read each column more than once: the sample proposes, the full column verifies (see *How the
  decision is made*). Except for `'5%'` on 10,000,000 rows they are therefore slower than with the previous version,
  which read only the sample and could propose types that round, truncate or reject values outside it. A sample
  still saves time on large tables: on 10,000,000 rows `'1%'` took 38 s instead of 76 s for `'100%'`.
- **Tables with at most 500,000 rows** are read completely when the sample covers at least 5% of them (*Automatic
  full scan*); on the same cluster this was not slower than the sampled path (see the table there).
- A table with a proposal that changes the values (`DECIMAL`, `DOUBLE`, `DATE`, `TIMESTAMP`, intervals) is read
  once more for the blank-padding / number-form notes, and each PRIMARY KEY with such a column once more for the
  duplicate-key check. On the tables above (no PRIMARY KEY) that extra read took about 0.6 s per 1,000,000 rows;
  it is included in the times.


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
