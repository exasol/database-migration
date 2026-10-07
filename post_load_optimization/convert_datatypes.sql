create schema if not exists database_migration;
/*
    This script creates datatype optimizations for you. You can run this after
    importing your data. Selecting smaller datatypes might improve performance.

    This script:
  - looks at all columns of type 'DOUBLE' and converts them directly to the
    smallest fitting DECIMAL if the values are exactly representable: integer values
    -> DECIMAL(p,0), or values with a small constant number of decimals (e.g. prices)
    -> DECIMAL(p,s). The conversion is only proposed when a round-trip cast proves it
    is lossless for every value (an exact check against the real target type, no tolerance); genuine
    floating-point values stay DOUBLE. Like a scale reduction (see the WARNING below), the target type is derived
    from today's values: later loads with more decimals are rounded silently, too many integer digits are rejected.
  - looks at all columns of type 'DECIMAL' (scale = 0) and converts them to a
    smaller integer type (32-bit / 64-bit) if a smaller datatype is sufficient.
  - looks at all columns of type 'DECIMAL' (scale <> 0) and converts them to a
    smaller decimal if a smaller datatype is sufficient. By default the scale is
    preserved. With reduce_decimal_scale = TRUE (recommended) the scale is ALSO reduced when every
    value uses fewer decimals (see SCALE REDUCTION below and its WARNING) - this is what shrinks the
    common DECIMAL(36,18) money / count columns.
  - looks at all columns of type 'TIMESTAMP' and 'TIMESTAMP WITH LOCAL TIME ZONE'
    and converts them to DATE if only date values (no time component) are contained
    in the column. For TIMESTAMP WITH LOCAL TIME ZONE the check and the conversion are
    evaluated in the current SESSIONTIMEZONE (both in the same session, so consistent).
  - looks at all columns of type 'VARCHAR' and converts them to a smaller
    VARCHAR if a smaller VARCHAR can still hold the information in the column.
    The original character set (ASCII / UTF8) is always preserved.

    Each conversion type can be switched on/off individually via the convert_double / convert_integer /
    convert_decimal / convert_timestamp / convert_varchar parameters (true = check & possibly convert this
    type, false = skip it entirely); reduce_decimal_scale extends convert_decimal.

    Storage classes: Exasol stores DECIMAL with precision <= 9 in 32 bit, <= 18 in 64 bit and above that in
    128 bit. Measured on 5 million rows, DECIMAL(36,18) -> DECIMAL(18,s) needs about 3x less (compressed)
    memory and speeds up GROUP BY / JOIN on such columns by about 1.7x. 64 -> 32 bit only halves the raw size.

    SCALE REDUCTION (reduce_decimal_scale = TRUE - recommended -, only together with convert_decimal = TRUE):
      * The smallest number of decimals that every value actually uses is measured EXACTLY (exact DECIMAL
        arithmetic: value = TRUNC(value, s)); a value is never rounded.
      * The new scale is the next step of the ladder 0 / 2 / 4 / 6 / 9 / 12 / 18 at or above that measured
        number (never above the current scale) - e.g. amounts whose values currently only end in .0 or .5
        get scale 2, not 1. The precision is the smallest of 9 / 18 that holds integer digits + new scale.
      * The scale is only changed when that reaches a SMALLER storage class than keeping the scale would (e.g.
        128 -> 64 / 32 bit, 64 -> 32 bit); otherwise it is kept (no risk without a gain).
      * Columns whose values use all their decimals (e.g. data that was loaded from DOUBLE into
        DECIMAL(36,18) carries binary noise such as 12.959999999983222784) keep their scale; their precision
        is still reduced when the integer digits + the current scale fit (e.g. fraction-only data -> DECIMAL(18,18)).
      * All scale reductions are listed in their own output section "SCALE REDUCTIONS".
      !!! WARNING: after a scale reduction (and likewise after a DOUBLE -> DECIMAL(p,s) conversion) Exasol ROUNDS
          SILENTLY every later INSERT / IMPORT / MERGE / UPDATE value that has more decimals than the new scale
          (e.g. 7.777 -> 7.78 for scale 2). Values with too many integer digits are rejected with an error. Only
          reduce the scale when you know that the source never delivers more decimals. Views on the column keep
          working but return narrower DECIMAL types, and DOUBLE results computed from it (e.g. value / 3, AVG) can
          differ in the last binary digit.

    Column DEFAULT and IDENTITY are taken into account:
      * a literal DEFAULT is treated like one more value of the column, so the target type always holds it (also a
        quoted number such as '0', and a number on a VARCHAR column); a DOUBLE column is kept when its literal
        DEFAULT has more digits than a DOUBLE holds (as DECIMAL it would give a different value);
      * DEFAULT NULL (and DEFAULT '', which Exasol stores as NULL) is the same as no DEFAULT;
      * a non-literal DEFAULT (e.g. CURRENT_USER) on a DECIMAL / DOUBLE / VARCHAR column keeps the column;
      * a TIMESTAMP column whose DEFAULT supplies a time of day (CURRENT_TIMESTAMP, SYSTIMESTAMP, NOW, ...) is
        never converted to DATE (a DATE default such as CURRENT_DATE or DATE '2024-01-01' is fine);
      * a TIMESTAMP column whose DEFAULT is written as a string (e.g. DEFAULT '2024-01-01') is kept: the string is
        parsed with the session's NLS date / timestamp format, so its meaning can differ between sessions;
      * an IDENTITY column is never shrunk below DECIMAL(18,0) and always keeps at least one digit of headroom
        above its current counter value, so the counter can keep growing.

    Only real, local base TABLE columns are inspected:
      * views and synonyms are excluded (COLUMN_OBJECT_TYPE = 'TABLE')
      * VIRTUAL SCHEMA columns are excluded (COLUMN_IS_VIRTUAL = FALSE)

    The "conversion" column always names the EXACT current data type on the left and the EXACT target type
    on the right (e.g. DECIMAL(20, 0) --> DECIMAL(9, 0), TIMESTAMP(6) WITH LOCAL TIME ZONE --> DATE,
    VARCHAR(200) ASCII --> VARCHAR(20) ASCII). With log_for_all_columns = TRUE every inspected column of an
    enabled type is listed, including a "Keep ..." row (with the reason) for columns that are kept.

    FOREIGN KEY handling (automatic): in Exasol a type change on a PRIMARY/FOREIGN KEY column fails unless
    the linked PK and FK columns are changed to the SAME type (same precision AND scale). So when FOREIGN KEYs
    touch the analyzed tables, the script (a) harmonizes every referential key group to ONE common target type
    that fits all of its columns, and (b) wraps the output / execution in a "DROP FOREIGN KEYS" step (first)
    and a "RE-ADD FOREIGN KEYS" step (last) - each FK is re-added in its ORIGINAL ENABLE/DISABLE state. A key
    column whose group reaches a table OUTSIDE the current filter is kept unchanged: when the columns in the
    filter could be converted (or are empty), a note names the other table(s) and suggests a re-run that includes
    them - a key group is only checked and converted as a whole (this note is always shown, also with
    log_for_all_columns = FALSE); otherwise the Keep row gives the real reason and names the other tables. A key
    group whose columns are all empty is kept (an empty column gives no information about the values that
    will be loaded later). When a key group is kept, a member kept for its own reason shows that reason and the
    other members name it (e.g. the member whose DEFAULT supplies a time of day). Foreign keys are read from
    EXA_DBA_CONSTRAINTS when the user may read it (so FKs on tables the user cannot see are known as well),
    otherwise from EXA_ALL_CONSTRAINTS - then the user must be able to see every referencing table. With no
    FOREIGN KEYs on the analyzed tables the run is unchanged (one cheap catalog check is the only overhead).
    When the FOREIGN KEY catalog cannot be read at all (e.g. QUERY_TIMEOUT reached), the script stops with an
    error instead of proposing key column changes without the FK handling.

    A column whose check query fails (e.g. no SELECT privilege on its table) is kept and listed as
    "Keep ... (check failed: <error>)" - also with log_for_all_columns = FALSE, so a failed check is never hidden.
    A QUERY_TIMEOUT reached during the run ends it with the FOREIGN KEY catalog stop described above.

    ====================================================================================
    !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!  A T T E N T I O N  !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    ====================================================================================
    The parameter apply_conversion = TRUE will IRREVERSIBLY change your table
    definitions with ALTER TABLE ... MODIFY statements.

      -> ONLY use apply_conversion = TRUE when you have reviewed the dry-run output
         (apply_conversion = FALSE) and are 100% sure that EVERY proposed conversion
         must really be performed.
      -> The SAFER way is to run with apply_conversion = FALSE first, then copy the
         generated statements from the "query_text" column and execute them YOURSELF,
         one statement at a time, checking each result. Run the statements IN THE ORDER
         shown (DROP FOREIGN KEYS first, the MODIFYs, then RE-ADD FOREIGN KEYS).
      -> apply_conversion = TRUE is ALL-OR-NOTHING: the script first COMMITs any pending work of your
         session, then runs the analysis and all statements (in the shown order) in ONE transaction. At the
         first error it ROLLs BACK everything it executed (incl. dropped FOREIGN KEYs) and stops; the
         "success" column shows the failing statement and which statements were rolled back / not executed.
         A successful run is COMMITted, also when your client runs without autocommit.
      -> Do not run it while the analysed tables are being loaded. A concurrent change that reaches a table
         before the script alters it ends in a transaction collision, which is reported as an error and rolls
         the whole run back (no value is converted unchecked). A write that arrives after the script has altered
         the table waits for the script's COMMIT and is then stored with the NEW type, i.e. more decimals are
         rounded like in any later load (see the WARNING above).
      -> Only an explicit TRUE switches a parameter on; NULL counts as FALSE (no accidental apply).
      -> The apply needs ALTER on every table it changes (and REFERENCES for re-added FOREIGN KEYs); a missing
         privilege is not checked in advance - the statement fails and the whole run is rolled back.

    Always have a backup / be able to recreate the affected tables before applying.
    ====================================================================================

    Name filters and case sensitivity:
      All schema/table/column names are processed as DELIMITED (double-quoted)
      identifiers via quote() and the ::identifier placeholder, so MixedCase and
      lowercase names work correctly. The schema_name / table_name FILTER is compared
      against the (case-exact) catalog names: a value WITHOUT an unescaped '%' is an EXACT name (an '_' in it is
      a normal character), a value WITH an unescaped '%' is a LIKE pattern (where '_' matches any single
      character). A backslash escapes a following % / _ / backslash in both forms (so MY\_SCHEMA still means
      MY_SCHEMA, and MY\_SCHEMA% matches only names starting with MY_SCHEMA); any other backslash is an ordinary
      character, so a name that really contains a backslash matches itself.
*/

--parameter	schema_name: 	  SCHEMA name (exact) or SCHEMA_FILTER with % (LIKE pattern); a backslash escapes % / _ / backslash
--parameter	table_name: 	  TABLE name (exact) or TABLE_FILTER with % (LIKE pattern); a backslash escapes % / _ / backslash
--parameter	convert_double:    true/false - check/convert DOUBLE      -> smallest fitting DECIMAL(p,0) / DECIMAL(p,s) (later loads with more decimals are rounded)
--parameter	convert_integer:   true/false - check/convert DECIMAL(p,0) -> DECIMAL(9,0) / DECIMAL(18,0)
--parameter	convert_decimal:   true/false - check/convert DECIMAL(p,s) -> DECIMAL(9,s) / DECIMAL(18,s) (scale kept)
--parameter	reduce_decimal_scale: true/false (true recommended) - with convert_decimal: ALSO reduce the scale when every value uses fewer decimals (lossless), e.g. DECIMAL(36,18) -> DECIMAL(9,2). WARNING - later loads with more decimals are rounded silently.
--parameter	convert_timestamp: true/false - check/convert TIMESTAMP / TIMESTAMP WITH LOCAL TIME ZONE -> DATE
--parameter	convert_varchar:   true/false - check/convert VARCHAR(n)   -> smaller VARCHAR (same charset)
--parameter	log_for_all_columns: true/false - false = report only columns that change (plus columns whose check failed and FK out-of-filter notes); true = report every inspected column (incl. 'Keep ...' rows)
--parameter	apply_conversion: !!! ATTENTION !!! true or false. TRUE irreversibly alters your tables (all-or-nothing, rollback at the first error) - only use it when you are 100% sure after reviewing the dry-run (false). The safer way is to review the output of a false-run and execute the statements manually, one by one.
--/
create or replace script database_migration.convert_datatypes(schema_name, table_name, convert_double, convert_integer, convert_decimal, reduce_decimal_scale, convert_timestamp, convert_varchar, log_for_all_columns, apply_conversion) RETURNS TABLE
 as

------------------------------------------------------------------------------------------------------
-- Small helpers
------------------------------------------------------------------------------------------------------
-- SQL NULL arrives as null (not nil) in Exasol Lua.
function isnull(v) return v == nil or v == null end

-- Converts a query value (Lua number or Exasol decimal) into a plain Lua number (integer when integral).
function num(v)
	if isnull(v) then return nil end
	local n = tonumber(tostring(v))
	if n ~= nil and math.tointeger ~= nil and math.tointeger(n) ~= nil then return math.tointeger(n) end
	return n
end

-- Name filter: returns the comparison operator and the value to bind.
--   * A value with an UNESCAPED '%' is a LIKE pattern ('_' = any single character); a backslash escapes the next
--     % / _ / backslash, and a lone backslash is taken literally. The pattern is used with ESCAPE chr(92), so it
--     does not depend on the session parameter DEFAULT_LIKE_ESCAPE_CHARACTER.
--   * Any other value is an EXACT name; escapes are removed first, so the escaped form MY\_SCHEMA still means
--     MY_SCHEMA, and a name that really contains a backslash (e.g. B\K) matches itself.
function name_filter(value)
	if isnull(value) then return '=', value end
	value = tostring(value)
	local bs = string.char(92)
	local pat, exact, has_pct = {}, {}, false
	local i, n = 1, #value
	while i <= n do
		local ch = string.sub(value, i, i)
		if ch == bs then
			local nx = string.sub(value, i + 1, i + 1)
			if nx == '%' or nx == '_' or nx == bs then
				pat[#pat+1] = bs..nx; exact[#exact+1] = nx; i = i + 2
			else
				pat[#pat+1] = bs..bs; exact[#exact+1] = bs; i = i + 1      -- lone backslash: a literal backslash
			end
		else
			if ch == '%' then has_pct = true end
			pat[#pat+1] = ch; exact[#exact+1] = ch; i = i + 1
		end
	end
	if has_pct then return 'like', table.concat(pat) end
	return '=', table.concat(exact)
end

-- SQL predicate for a name filter (op from name_filter).
function filter_pred(column, param, op)
	if op == 'like' then return column..[[ like :]]..param..[[ escape chr(92)]] end
	return column..[[ = :]]..param
end

-- Number of integer digits of |c| (a value below 1 has 0 integer digits), maximum over all values.
function int_digits_sql(c)
	return 'coalesce(max(case when abs('..c..') < 1 then 0 else length(cast(floor(abs('..c..')) as decimal(36,0))) end), 0)'
end

-- Smallest number of decimals s (0 .. maxs) with c = TRUNC(c, s) for every value - exact DECIMAL arithmetic.
-- NULLs carry no decimals (without the first branch they would fall through to the ELSE).
function min_scale_sql(c, maxs)
	if maxs <= 0 then return '0' end
	local t = 'coalesce(max(case when '..c..' is null then 0'
	for s = 0, maxs - 1 do t = t..' when '..c..' = trunc('..c..', '..s..') then '..s end
	return t..' else '..maxs..' end), 0)'
end

-- The rows a column statistic is computed over: the stored values (__CDT_D = 1) plus - if the column has a
-- literal DEFAULT - that default as one extra value (__CDT_D = 0), so every chosen target also holds the default.
-- The analysed column is aliased as __CDT_V (use the constant V in the statistics), so no source column name
-- can collide with the helper columns.
V = '"__CDT_V"'
function value_source(src_type, def)
	local s = [[(select ::col_name as "__CDT_V", 1 as "__CDT_D" from ::curr_schema.::curr_table]]
	if def ~= nil then
		s = s..[[ union all select cast((]]..def..[[) as ]]..src_type..[[), 0 from sys.dual]]
	end
	return s..[[) v]]
end

-- The column DEFAULT as a usable string, or nil when there is none. DEFAULT NULL (and DEFAULT '', which Exasol
-- stores as NULL) behaves exactly like no default.
function normalize_default(d)
	if isnull(d) then return nil end
	local t = string.gsub(string.gsub(tostring(d), '^%s+', ''), '%s+$', '')
	if t == '' or string.upper(t) == 'NULL' then return nil end
	return t
end

-- The numeral of a numeric-literal DEFAULT as the catalog shows it (e.g. 1.5, -0.5, 1250, 1.25E20, or a quoted
-- numeral such as '1.5'), returned UNQUOTED - a numeric literal is evaluated independently of
-- NLS_NUMERIC_CHARACTERS, a quoted string is not. nil when the DEFAULT is not a numeric literal.
function numeric_literal(d)
	local x = string.gsub(d, '^%s+', '')
	x = string.gsub(x, '%s+$', '')
	local inner = string.match(x, "^'(.*)'$")      -- a quoted numeral such as '1.5' (as MySQL-style DDL writes it)
	if inner ~= nil then x = string.gsub(string.gsub(inner, '^%s+', ''), '%s+$', '') end
	local body = string.gsub(x, '^[%+%-]', '')
	body = string.gsub(body, '[eE][%+%-]%d+$', '')
	body = string.gsub(body, '[eE]%d+$', '')
	if string.match(body, '^%d+$') ~= nil or string.match(body, '^%d*%.%d+$') ~= nil or string.match(body, '^%d+%.%d*$') ~= nil then
		return x
	end
	return nil
end

-- Length (in characters) of a string-literal DEFAULT such as 'N/A'; nil when the DEFAULT is not a plain literal.
function string_literal_length(d)
	if string.match(d, "^'.*'$") == nil then return nil end
	local raw = string.sub(d, 2, -2)
	if string.find(string.gsub(raw, "''", ''), "'", 1, true) ~= nil then return nil end   -- e.g. 'a'||'b'
	local inner = string.gsub(raw, "''", "'")
	if utf8 ~= nil and utf8.len(inner) ~= nil then return utf8.len(inner) end
	return #inner
end

-- True when a TIMESTAMP column DEFAULT yields (or may yield) a time of day other than midnight.
function default_has_time(d)
	local u = string.upper(d)
	for _, f in ipairs({'CURRENT_TIMESTAMP', 'SYSTIMESTAMP', 'LOCALTIMESTAMP', 'NOW'}) do
		if string.find(u, f, 1, true) ~= nil then return true end
	end
	if u == 'CURRENT_DATE' or u == 'SYSDATE' then return false end
	if string.match(u, '^TO_DATE%(') ~= nil then return false end    -- TO_DATE yields a DATE (catalog form of DEFAULT DATE '...')
	local hms = string.match(u, "^TIMESTAMP '%d+%-%d+%-%d+ ([%d%.:]+)'$")
	if hms ~= nil and string.match(hms, '^[0%.:]+$') ~= nil then return false end
	return true   -- a time part, or an expression we cannot judge: keep the TIMESTAMP (conservative)
end

------------------------------------------------------------------------------------------------------
-- Returns the smaller integer precision (9 = 32-bit, 18 = 64-bit) that still fits
-- "needed" digits, or nil if no smaller type than current_precision is possible.
------------------------------------------------------------------------------------------------------
function smaller_integer_precision(needed, current_precision)
	if needed <= 9 and current_precision > 9 then
		return 9     -- fits into 32-bit
	elseif needed <= 18 and current_precision > 18 then
		return 18    -- fits into 64-bit
	else
		return nil   -- no smaller type fits
	end
end

-- Storage class of a DECIMAL precision: 1 = 32 bit, 2 = 64 bit, 3 = 128 bit.
function storage_class(p)
	if p <= 9 then return 1 elseif p <= 18 then return 2 else return 3 end
end

-- Scale ladder for reduce_decimal_scale: the smallest step at or above the measured minimal scale,
-- never above the current scale.
SCALE_LADDER = {0, 2, 4, 6, 9, 12, 18}
function ladder_scale(smin, current_scale)
	for _, l in ipairs(SCALE_LADDER) do
		if l >= smin then return math.min(l, current_scale) end
	end
	return current_scale
end

------------------------------------------------------------------------------------------------------
-- Target for a DECIMAL(p,s) column or key group, from its integer digits and its measured minimal scale.
-- Returns target_precision, target_scale, scale_reduced -- or nil when no smaller type is possible.
-- The scale is only reduced (reduce = true) when that reaches a smaller storage class than keeping it.
------------------------------------------------------------------------------------------------------
function decide_decimal(p, s, int, smin, reduce)
	local t1 = smaller_integer_precision(int + s, p)          -- scale kept (precision only)
	if reduce and smin ~= nil then
		local s2 = ladder_scale(smin, s)
		if s2 < s then
			local t2 = smaller_integer_precision(int + s2, p)
			if t2 ~= nil and storage_class(t2) < storage_class(t1 or p) then return t2, s2, true end
		end
	end
	if t1 ~= nil then return t1, s, false end
	return nil
end

------------------------------------------------------------------------------------------------------
-- DOUBLE -> DECIMAL(p,s) round trip against the EXACT target type: true when every value (and a literal
-- DEFAULT) converts to DECIMAL(p,s) and back to the identical DOUBLE. The real target precision is used on
-- purpose: the check must prove exactly the conversion that ALTER ... MODIFY performs (to DECIMAL(9|18,s));
-- a round trip through another precision proves nothing about the real target. A cast error (value out of
-- the target range) also returns false.
------------------------------------------------------------------------------------------------------
function double_roundtrip_ok(scm, tbl, col, defsql, p, s)
	local ok, r = pquery([[select count(case when cast(cast(]]..V..[[ as decimal(]]..p..[[,]]..s..[[)) as double) <> ]]..V..[[ then 1 end) from ]]..value_source('DOUBLE', defsql),
		{curr_schema=scm, curr_table=tbl, col_name=col})
	return ok and #r == 1 and num(r[1][1]) == 0
end

-- A literal DEFAULT of a DOUBLE column is stored as DOUBLE today, but after the MODIFY Exasol evaluates the literal
-- directly as DECIMAL(p,s). Both must give the same value - a literal with more significant digits than a DOUBLE
-- holds (e.g. 9007199254740993) would otherwise fill later default rows with a slightly different value.
function double_default_ok(defsql, p, s)
	if defsql == nil then return true end
	local ok, r = pquery('select case when cast(('..defsql..') as decimal('..p..','..s..')) = cast(cast(('..defsql..') as double) as decimal('..p..','..s..')) then 1 else 0 end from sys.dual')
	return ok and #r == 1 and num(r[1][1]) == 1
end

-- A DEFAULT written as a quoted string (e.g. '1.5') is converted again by ALTER ... MODIFY, with the NLS settings
-- of the session (NLS_NUMERIC_CHARACTERS, ...). True when it converts to the target type in this session.
function quoted_default_ok(def, ttype)
	if def == nil or string.sub(def, 1, 1) ~= "'" then return true end
	local ok = pquery('select cast(('..def..') as '..ttype..') as x')
	return ok
end

-- Smallest DECIMAL precision class (9 or 18) for integer digits + scale; nil above 18 (never larger than a DOUBLE).
function double_target_precision(int, s)
	if int + s <= 9 then return 9 elseif int + s <= 18 then return 18 end
	return nil
end

------------------------------------------------------------------------------------------------------
-- For a DOUBLE column that also contains fractional values, find the smallest scale s (1..MAX_DOUBLE_SCALE)
-- at which EVERY non-null value is exactly representable as DECIMAL(p,s), proven per row by a round-trip cast
-- against the REAL target class:  cast(cast(value as decimal(p,s)) as double) = value .
-- p = 9 is tried first (when integer digits + s <= 9), then p = 18, so every candidate is checked against
-- exactly the type the ALTER ... MODIFY will produce (a round trip through another precision proves nothing
-- about the real target).
-- Integer digits + s must fit into 18 (64-bit), so the DECIMAL is never larger than the 8-byte DOUBLE.
-- All candidates are measured in ONE scan; if a cast overflows there (a value at the very top of the range), the
-- candidates are tested one by one. Returns scale, precision -- or nil if none.
------------------------------------------------------------------------------------------------------
function detect_lossless_double_scale(scm, tbl, col, defsql, max_int_digits)
	local MAX_DOUBLE_SCALE = 9    -- conservative cap (prices, rates, ...); higher-scale data stays DOUBLE
	local cand, expr = {}, {}
	for s = 1, MAX_DOUBLE_SCALE do
		for _, p in ipairs({9, 18}) do
			if max_int_digits + s <= p then
				cand[#cand+1] = {s=s, p=p}
				expr[#expr+1] = "count(case when cast(cast("..V.." as decimal("..p..","..s..")) as double) <> "..V.." then 1 end)"
			end
		end
	end
	if #cand == 0 then return nil end
	local ok, r = pquery([[select ]]..table.concat(expr, ', ')..[[ from ]]..value_source('DOUBLE', defsql),
		{curr_schema=scm, curr_table=tbl, col_name=col})
	for i, c in ipairs(cand) do      -- candidates are ordered by scale, then precision 9 before 18
		local lossless
		if ok then lossless = (num(r[1][i]) == 0) else lossless = double_roundtrip_ok(scm, tbl, col, defsql, c.p, c.s) end
		if lossless then return c.s, c.p end
	end
	return nil
end

-- Takes a number as input, adds 20% headroom and rounds up to the next bigger round decimal.
function estimate_optimal_varchar_length(n)
	local n_p20            = math.ceil(n + n * 0.2)               -- add 20% so everything fits in
	local number_digits    = string.len(n_p20)
	local magnitude        = math.floor(10 ^ (number_digits - 1))
	local estimated_length = math.floor(n_p20 / magnitude) * magnitude + magnitude
	return math.min(estimated_length, 2000000)                   -- maximal varchar size is 2,000,000
end

------------------------------------------------------------------------------------------------------
-- Renders the EXACT target type string from a harmonized group target (or a per-column target).
------------------------------------------------------------------------------------------------------
function render_target(t)
	if     t.fam == 'DEC0' then return "DECIMAL("..t.target..", 0)"
	elseif t.fam == 'DECS' then return "DECIMAL("..t.target..", "..t.s..")"
	elseif t.fam == 'DBL'  then return "DECIMAL("..t.p..", "..t.s..")"
	elseif t.fam == 'TS'   then return "DATE"
	elseif t.fam == 'VC'   then return "VARCHAR("..t.target..") "..t.cs
	else                        return nil end -- KEEP
end

------------------------------------------------------------------------------------------------------
-- Decides ONE common target for a referential key group (all members share the same source type,
-- because an FK requires identical types). Returns a target descriptor, or {fam='KEEP'} with either
-- culprit = the member whose own analysis keeps it (its reason is shown to the others) or why = the reason
-- (then the columns stay as they are and the FK remains valid).
-- Empty members add no data constraint (only their DEFAULT, if any); a group whose members are ALL
-- empty is kept, like an empty column outside a key group.
------------------------------------------------------------------------------------------------------
function group_target(members, reduce)
	local any_data = false
	for i = 1, #members do
		if members[i].tgt.fam == 'KEEP' then return {fam='KEEP', culprit=members[i]} end
		if not members[i].tgt.empty then any_data = true end
	end
	if not any_data then return {fam='KEEP', why='all key columns are empty'} end
	local fam = members[1].tgt.fam
	if fam == 'DEC0' then
		local need, ident = 0, false
		for i = 1, #members do
			if members[i].tgt.need > need then need = members[i].tgt.need end
			if members[i].tgt.identity then ident = true end
		end
		local target = smaller_integer_precision(need, members[1].tgt.p)
		if ident and target == 9 then
			if members[1].tgt.p > 18 then target = 18 else target = nil end
		end
		if target == nil then
			if ident and need <= 18 then return {fam='KEEP', why='an IDENTITY column is never shrunk below 18 digits'} end
			if ident then return {fam='KEEP', why='an IDENTITY counter needs headroom, needed digits: '..need} end
			return {fam='KEEP', why='no common smaller type, needed digits: '..need}
		end
		if ident then
			return {fam='DEC0', target=target, facts='needed digits: '..need..' (incl. IDENTITY counter headroom - never below 18 digits)'}
		end
		return {fam='DEC0', target=target, facts='max length: '..need}
	elseif fam == 'DECS' then
		local int, smin = 0, 0
		for i = 1, #members do
			if members[i].tgt.int > int then int = members[i].tgt.int end
			if members[i].tgt.smin > smin then smin = members[i].tgt.smin end
		end
		local target, s, reduced = decide_decimal(members[1].tgt.p, members[1].tgt.s, int, smin, reduce)
		local facts = 'needed precision: '..(int + members[1].tgt.s)
		if reduce then facts = 'integer digits: '..int..', decimals used: '..smin end
		if target == nil then
			if reduce then return {fam='KEEP', why='no common smaller type, '..facts} end
			return {fam='KEEP', why='no common smaller type, '..facts..'; scale kept - reduce_decimal_scale = true checks a lossless scale reduction'}
		end
		return {fam='DECS', target=target, s=s, reduced=reduced, facts=facts}
	elseif fam == 'VC' then
		local ml = 0
		for i = 1, #members do if members[i].tgt.maxlen > ml then ml = members[i].tgt.maxlen end end
		local tl = estimate_optimal_varchar_length(ml)
		if tl < members[1].tgt.n then return {fam='VC', target=tl, cs=members[1].tgt.cs, facts='max length: '..ml} end
		return {fam='KEEP', why='no common smaller type, max length: '..ml}
	elseif fam == 'TS' then
		for i = 1, #members do if members[i].tgt.has_time then return {fam='KEEP', culprit=members[i]} end end
		return {fam='TS'}
	elseif fam == 'DBL' then
		local int, s = 0, 0
		for i = 1, #members do
			if not members[i].tgt.conv then return {fam='KEEP', culprit=members[i]} end
			if members[i].tgt.int > int then int = members[i].tgt.int end
			if members[i].tgt.s > s then s = members[i].tgt.s end
		end
		local p
		if int + s <= 9 then p = 9 elseif int + s <= 18 then p = 18 else
			return {fam='KEEP', why='no common DECIMAL with at most 18 digits, integer digits: '..int..', scale: '..s}
		end
		return {fam='DBL', p=p, s=s, facts='integer digits: '..int..', scale: '..s}
	end
	return {fam='KEEP', why='no common smaller type'}
end

-- The bare reason of a record's own Keep message ('Keep DOUBLE (non-literal DEFAULT 1+1 - not changed)'
-- -> 'non-literal DEFAULT 1+1').
function bare_reason(a)
	local m = a.message
	if string.sub(m, 1, #('Keep '..a.src)) == 'Keep '..a.src then m = string.sub(m, #('Keep '..a.src) + 1) end
	m = string.gsub(m, '^, ', '')
	m = string.gsub(m, '^%s+', '')
	if string.sub(m, 1, 1) == '(' and string.sub(m, -1) == ')' then m = string.sub(m, 2, -2) end
	m = string.gsub(m, ' %- not changed$', '')
	return m
end

-- True when a key column is kept for its OWN reason (it would also be kept outside the key group);
-- such a column keeps its own Keep message. Empty columns are not counted: the group decides for them.
function own_keep(a)
	return (not a.modify) and not (a.tgt ~= nil and a.tgt.empty)
end

-- Human-readable reason of a KEEP group target, for the members' Keep rows.
function group_keep_reason(gt)
	if gt.culprit ~= nil then
		local c = gt.culprit
		return 'key column '..quote(c.sch)..'.'..quote(c.tab)..'.'..quote(c.col)..' is kept ('..bare_reason(c)..')'
	end
	return gt.why or 'no common smaller type'
end

------------------------------------------------------------------------------------------------------
-- DOUBLE --> DECIMAL
------------------------------------------------------------------------------------------------------
function convert_double_to_decimal(schema_name, table_name)
	local result_table = {}
	local res = query([[
			select column_schema, column_table, column_name, column_default
			from   sys.exa_all_columns
			where  column_type_id = 8 and             -- type_id of DOUBLE
			       ]]..filter_pred('column_schema', 'schema_filter', SF_OP)..[[ and
			       ]]..filter_pred('column_table', 'table_filter', TF_OP)..[[ and
			       column_object_type = 'TABLE' and
			       column_is_virtual  = FALSE
		]], {schema_filter=SF_VAL, table_filter=TF_VAL})

	for i=1,#res do
		local modify_column, message_action, query_to_execute = false, '', ''
		local tgt = {fam='KEEP'}
		local scm, tbl, col = quote(res[i][1]), quote(res[i][2]), quote(res[i][3])
		local src = 'DOUBLE'
		local def = normalize_default(res[i][4])
		local defsql = nil
		if def ~= nil then defsql = numeric_literal(def) end

		if def ~= nil and defsql == nil then
			message_action = 'Keep DOUBLE (non-literal DEFAULT '..def..' - not changed)'
		else
			-- exact integer test (x = floor(x) is exact in DOUBLE arithmetic); the target is verified below
			local tsSuc, tsColumns = pquery([[
				select
					count(case when "__CDT_D" = 1 then ]]..V..[[ end) as values_in_column,
					count(case when ]]..V..[[ <> floor(]]..V..[[) then 1 end) as non_integer_rows,
					]]..int_digits_sql(V)..[[ as max_int_length
				from ]]..value_source('DOUBLE', defsql), {curr_schema=scm, curr_table=tbl, col_name=col})

			if not tsSuc then
				if string.find(string.lower(tsColumns.error_message), 'out of range', 1, true) == nil then
					message_action = 'Keep DOUBLE (check failed: '..tsColumns.error_message..')'
				elseif def ~= nil then
					message_action = 'Keep DOUBLE (values or its DEFAULT '..def..' exceed the DECIMAL(36) range)'
				else
					message_action = 'Keep DOUBLE (values exceed the DECIMAL(36) range)'
				end
			else
				local values, non_integer, max_int_length = num(tsColumns[1][1]), num(tsColumns[1][2]), num(tsColumns[1][3])
				local empty = (values == 0)
				if non_integer == 0 then
					local target = double_target_precision(max_int_length, 0)
					if target == 9 then         -- every precision is verified at most once
						if not double_roundtrip_ok(scm, tbl, col, defsql, 9, 0) then target = 18 end
						if target == 18 and not double_roundtrip_ok(scm, tbl, col, defsql, 18, 0) then target = nil end
					elseif target == 18 and not double_roundtrip_ok(scm, tbl, col, defsql, 18, 0) then
						target = nil
					end
					local default_differs = false
					if target ~= nil and not (double_default_ok(defsql, target, 0) and quoted_default_ok(def, 'DECIMAL('..target..',0)')) then
						target = nil; default_differs = true
					end
					if target ~= nil then
						tgt = {fam='DBL', conv=true, int=max_int_length, s=0, empty=empty, defsql=defsql, hasdef=(defsql ~= nil), def=def}
						if empty then
							message_action = 'Keep DOUBLE (empty)'
						else
							query_to_execute = "ALTER TABLE "..scm.."."..tbl.." MODIFY ("..col.." DECIMAL("..target..",0));"
							modify_column    = true
							message_action   = src..' --> DECIMAL('..target..', 0), max length: '..math.max(max_int_length, 1)
							if def ~= nil then message_action = message_action..' (incl. DEFAULT '..def..')' end
						end
					elseif default_differs then
						message_action = 'Keep DOUBLE (its DEFAULT '..def..' does not convert to the same DECIMAL value - more digits than a DOUBLE holds, or the session NLS settings)'
					elseif max_int_length > 18 then
						message_action = 'Keep DOUBLE (integer values exceed DECIMAL(18,0), max length: '..max_int_length..')'
					else
						message_action = 'Keep DOUBLE (values at the top of the DECIMAL(18,0) range do not convert exactly, max length: '..max_int_length..')'
					end
				else
					local s, target = detect_lossless_double_scale(scm, tbl, col, defsql, max_int_length)
					local default_differs = false
					if s ~= nil and not (double_default_ok(defsql, target, s) and quoted_default_ok(def, 'DECIMAL('..target..','..s..')')) then
						s = nil; default_differs = true
					end
					if s ~= nil then
						tgt = {fam='DBL', conv=true, int=max_int_length, s=s, empty=empty, defsql=defsql, hasdef=(defsql ~= nil), def=def}
						if empty then
							message_action = 'Keep DOUBLE (empty)'
						else
							query_to_execute = "ALTER TABLE "..scm.."."..tbl.." MODIFY ("..col.." DECIMAL("..target..","..s.."));"
							modify_column    = true
							message_action   = src..' --> DECIMAL('..target..', '..s..') (lossless)'
							if def ~= nil then message_action = message_action..' (incl. DEFAULT '..def..')' end
						end
					elseif default_differs then
						message_action = 'Keep DOUBLE (its DEFAULT '..def..' does not convert to the same DECIMAL value - more digits than a DOUBLE holds, or the session NLS settings)'
					else
						message_action = 'Keep DOUBLE (not losslessly representable as DECIMAL(18,s) with s <= 9)'
					end
				end
			end
		end
		result_table[#result_table+1] = {sch=res[i][1], tab=res[i][2], col=res[i][3], src=src,
			message=message_action, query=query_to_execute, modify=modify_column, tgt=tgt}
	end
	return result_table
end

------------------------------------------------------------------------------------------------------
-- DECIMAL(p,0) --> smaller integer DECIMAL
------------------------------------------------------------------------------------------------------
function convert_integer_to_smaller_integer(schema_name, table_name)
	local result_table = {}
	local res = query([[
			select column_schema, column_table, column_name, column_num_prec, column_default, column_identity,
			       length(cast(abs(column_identity) as varchar(40))) as identity_length
			from   sys.exa_all_columns
			where  column_type_id = 3 and             -- type_id of DECIMAL
			       ]]..filter_pred('column_schema', 'schema_filter', SF_OP)..[[ and
			       ]]..filter_pred('column_table', 'table_filter', TF_OP)..[[ and
			       column_object_type = 'TABLE' and
			       column_is_virtual  = FALSE and
			       column_num_scale = 0                -- only columns without scale
		]], {schema_filter=SF_VAL, table_filter=TF_VAL})

	for i=1,#res do
		local modify_column, message_action, query_to_execute = false, '', ''
		local tgt = {fam='KEEP'}
		local scm, tbl, col = quote(res[i][1]), quote(res[i][2]), quote(res[i][3])
		local precision = num(res[i][4])
		local src = 'DECIMAL('..precision..', 0)'
		local def, ident = normalize_default(res[i][5]), res[i][6]

		if precision <= 9 then
			-- already the smallest integer type -> no scan needed
			message_action = 'Keep '..src..' (already minimal precision)'
		elseif def ~= nil and numeric_literal(def) == nil then
			message_action = 'Keep '..src..' (non-literal DEFAULT '..def..' - not changed)'
		else
			local defsql = nil
			if def ~= nil then defsql = numeric_literal(def) end
			local dOk, dColumns = pquery([[
				select count(case when "__CDT_D" = 1 then ]]..V..[[ end) as values_in_column,
				       coalesce(max(length(abs(]]..V..[[))), 0) as max_length
				from ]]..value_source(src, defsql), {curr_schema=scm, curr_table=tbl, col_name=col})
			if not dOk then
				message_action = 'Keep '..src..' (check failed: '..dColumns.error_message..')'
			else
			local not_null_count, max_length = num(dColumns[1][1]), num(dColumns[1][2])
			local is_identity = not isnull(ident)
			local need, note = max_length, ''
			if is_identity then
				-- the identity counter only grows: size for its current value plus one digit of headroom, and
				-- never go below 18 digits
				local ident_len = num(res[i][7])          -- digit count from SQL (exact also above 2^63)
				if ident_len + 1 > need then need = ident_len + 1 end
				note = ' (IDENTITY column, counter: '..ident_len..' digits - never below 18 digits, the counter keeps one digit of headroom)'
			end
			tgt = {fam='DEC0', p=precision, need=need, empty=(not_null_count == 0), identity=is_identity, hasdef=(def ~= nil), def=def}
			local incl = ''
			if def ~= nil then incl = ' (incl. DEFAULT '..def..')' end
			local target = smaller_integer_precision(need, precision)
			if is_identity and target == 9 then
				if precision > 18 then target = 18 else target = nil end
			end
			if target ~= nil and not quoted_default_ok(def, 'DECIMAL('..target..',0)') then
				note = note..' (its DEFAULT '..def..' does not convert to DECIMAL('..target..',0) with the session NLS settings)'
				target = nil
				tgt = {fam='KEEP'}
			end
			if not_null_count == 0 then
				message_action = 'Keep '..src..' (empty)'
			elseif target ~= nil then
				query_to_execute = "ALTER TABLE "..scm.."."..tbl.." MODIFY ("..col.." DECIMAL("..target..",0));"
				modify_column    = true
				message_action   = src..' --> DECIMAL('..target..', 0), max length: '..max_length..note..incl
			else
				message_action = 'Keep '..src..', max length: '..max_length..note..incl
			end
			end
		end
		result_table[#result_table+1] = {sch=res[i][1], tab=res[i][2], col=res[i][3], src=src,
			message=message_action, query=query_to_execute, modify=modify_column, tgt=tgt}
	end
	return result_table
end

------------------------------------------------------------------------------------------------------
-- DECIMAL(p,s) --> smaller DECIMAL (scale preserved, or - with reduce_decimal_scale - reduced losslessly)
------------------------------------------------------------------------------------------------------
function convert_decimal_with_scale_to_smaller_decimal(schema_name, table_name, reduce)
	local result_table = {}
	local res = query([[
			select column_schema, column_table, column_name, column_num_prec, column_num_scale, column_default
			from   sys.exa_all_columns
			where  column_type_id = 3 and             -- type_id of DECIMAL
			       ]]..filter_pred('column_schema', 'schema_filter', SF_OP)..[[ and
			       ]]..filter_pred('column_table', 'table_filter', TF_OP)..[[ and
			       column_object_type = 'TABLE' and
			       column_is_virtual  = FALSE and
			       column_num_scale <> 0               -- only columns that have a scale
		]], {schema_filter=SF_VAL, table_filter=TF_VAL})

	for i=1,#res do
		local modify_column, message_action, query_to_execute = false, '', ''
		local tgt = {fam='KEEP'}
		local scm, tbl, col = quote(res[i][1]), quote(res[i][2]), quote(res[i][3])
		local precision, col_scale = num(res[i][4]), num(res[i][5])
		local src = 'DECIMAL('..precision..', '..col_scale..')'
		local def = normalize_default(res[i][6])
		local scale_reduced = false

		if precision <= 9 then
			message_action = 'Keep '..src..' (already minimal precision)'
		elseif def ~= nil and numeric_literal(def) == nil then
			message_action = 'Keep '..src..' (non-literal DEFAULT '..def..' - not changed)'
		else
			local defsql = nil
			if def ~= nil then defsql = numeric_literal(def) end
			local smin_expr = '0'
			if reduce then smin_expr = min_scale_sql(V, col_scale) end
			local dOk, dColumns = pquery([[
				select count(case when "__CDT_D" = 1 then ]]..V..[[ end) as values_in_column,
				       ]]..int_digits_sql(V)..[[ as max_int_length,
				       ]]..smin_expr..[[ as min_scale
				from ]]..value_source(src, defsql), {curr_schema=scm, curr_table=tbl, col_name=col})
			if not dOk then
				message_action = 'Keep '..src..' (check failed: '..dColumns.error_message..')'
			else
			local not_null_count, max_int_length = num(dColumns[1][1]), num(dColumns[1][2])
			local smin = col_scale
			if reduce then smin = num(dColumns[1][3]) end
			local needed_precision = max_int_length + col_scale
			tgt = {fam='DECS', p=precision, s=col_scale, int=max_int_length, smin=smin, empty=(not_null_count == 0), hasdef=(def ~= nil), def=def}
			local target, new_scale, reduced = decide_decimal(precision, col_scale, max_int_length, smin, reduce)
			local nls_note = ''
			if target ~= nil and not quoted_default_ok(def, 'DECIMAL('..target..','..new_scale..')') then
				nls_note = ' (its DEFAULT '..def..' does not convert to DECIMAL('..target..', '..new_scale..') with the session NLS settings)'
				target = nil
				tgt = {fam='KEEP'}
			end
			local facts
			if reduce then
				facts = 'integer digits: '..max_int_length..', decimals used: '..smin
			else
				facts = 'needed precision: '..needed_precision
			end
			if def ~= nil then facts = facts..' (incl. DEFAULT '..def..')' end
			if not_null_count == 0 then
				message_action = 'Keep '..src..' (empty)'
			elseif target ~= nil then
				query_to_execute = "ALTER TABLE "..scm.."."..tbl.." MODIFY ("..col.." DECIMAL("..target..","..new_scale.."));"
				modify_column    = true
				scale_reduced    = reduced
				if reduced then
					message_action = src..' --> DECIMAL('..target..', '..new_scale..') [SCALE REDUCED], '..facts
				else
					message_action = src..' --> DECIMAL('..target..', '..new_scale..'), '..facts
				end
			elseif nls_note ~= '' then
				message_action = 'Keep '..src..', '..facts..nls_note
			elseif reduce then
				message_action = 'Keep '..src..', '..facts..' (no smaller type possible)'
			else
				message_action = 'Keep '..src..', '..facts..' (scale kept - reduce_decimal_scale = true checks a lossless scale reduction)'
			end
			end
		end
		result_table[#result_table+1] = {sch=res[i][1], tab=res[i][2], col=res[i][3], src=src,
			message=message_action, query=query_to_execute, modify=modify_column, tgt=tgt, scale_reduced=scale_reduced}
	end
	return result_table
end

------------------------------------------------------------------------------------------------------
-- TIMESTAMP / TIMESTAMP WITH LOCAL TIME ZONE --> DATE
------------------------------------------------------------------------------------------------------
function convert_timestamp_to_date(schema_name, table_name)
	local result_table = {}
	local res = query([[
			select column_schema, column_table, column_name, column_type, column_default
			from   sys.exa_all_columns
			where  column_type_id in (93, 124) and    -- 93 = TIMESTAMP, 124 = TIMESTAMP WITH LOCAL TIME ZONE
			       ]]..filter_pred('column_schema', 'schema_filter', SF_OP)..[[ and
			       ]]..filter_pred('column_table', 'table_filter', TF_OP)..[[ and
			       column_object_type = 'TABLE' and
			       column_is_virtual  = FALSE
		]], {schema_filter=SF_VAL, table_filter=TF_VAL})

	for i=1,#res do
		local modify_column, message_action, query_to_execute = false, '', ''
		local tgt = {fam='KEEP'}
		local scm, tbl, col = quote(res[i][1]), quote(res[i][2]), quote(res[i][3])
		local src = res[i][4]   -- exact current type from the catalog, e.g. "TIMESTAMP(6)" / "TIMESTAMP(3) WITH LOCAL TIME ZONE"
		local def = normalize_default(res[i][5])
		-- a quoted-string DEFAULT is parsed with NLS_TIMESTAMP_FORMAT today but would be parsed with NLS_DATE_FORMAT
		-- after a conversion to DATE (the MODIFY or later default inserts can fail): such a column is kept
		local default_string = (def ~= nil) and string.sub(def, 1, 1) == "'"
		local default_time = (def ~= nil) and (default_string or default_has_time(def))

		local tsSuc, tsColumns = pquery([[
			select count(::col_name) as values_in_column,
			       count(case when ::col_name <> TRUNC(::col_name) then 1 end) as rows_with_time
			from ::curr_schema.::curr_table
		]], {curr_schema=scm, curr_table=tbl, col_name=col})

		if not tsSuc then
			message_action = 'Keep '..src..' (check failed: '..tsColumns.error_message..')'
		else
			local values, with_time = num(tsColumns[1][1]), num(tsColumns[1][2])
			tgt = {fam='TS', has_time=(with_time > 0 or default_time), empty=(values == 0)}
			if values == 0 and default_time then
				message_action = 'Keep '..src..' (empty; its DEFAULT '..def..' may supply a time of day)'
			elseif values == 0 then
				message_action = 'Keep '..src..' (empty)'
			elseif with_time > 0 then
				message_action = 'Keep '..src..' (has a time component)'
			elseif default_string then
				message_action = 'Keep '..src..' (its DEFAULT '..def..' is a string - parsed with the session NLS date/timestamp format)'
			elseif default_time then
				message_action = 'Keep '..src..' (its DEFAULT '..def..' may supply a time of day)'
			else
				query_to_execute = "ALTER TABLE "..scm.."."..tbl.." MODIFY ("..col.." DATE);"
				modify_column    = true
				message_action   = src..' --> DATE'
			end
		end
		result_table[#result_table+1] = {sch=res[i][1], tab=res[i][2], col=res[i][3], src=src,
			message=message_action, query=query_to_execute, modify=modify_column, tgt=tgt}
	end
	return result_table
end

------------------------------------------------------------------------------------------------------
-- VARCHAR(n) --> smaller VARCHAR (charset preserved)
------------------------------------------------------------------------------------------------------
function convert_varchar_to_smaller_varchar(schema_name, table_name)
	local result_table = {}
	local res = query([[
			select column_schema, column_table, column_name, column_maxsize, column_type, column_default
			from   sys.exa_all_columns
			where  column_type_id = 12 and            -- type_id of VARCHAR
			       ]]..filter_pred('column_schema', 'schema_filter', SF_OP)..[[ and
			       ]]..filter_pred('column_table', 'table_filter', TF_OP)..[[ and
			       column_object_type = 'TABLE' and
			       column_is_virtual  = FALSE
		]], {schema_filter=SF_VAL, table_filter=TF_VAL})

	for i=1,#res do
		local modify_column, message_action, query_to_execute = false, '', ''
		local tgt = {fam='KEEP'}
		local scm, tbl, col = quote(res[i][1]), quote(res[i][2]), quote(res[i][3])
		local current_maxsize = num(res[i][4])
		local charset = 'UTF8'
		if string.find(string.upper(res[i][5]), 'ASCII', 1, true) ~= nil then charset = 'ASCII' end
		local src = 'VARCHAR('..current_maxsize..') '..charset
		local def = normalize_default(res[i][6])
		local deflen = 0
		if def ~= nil then
			deflen = string_literal_length(def)
			if deflen == nil and numeric_literal(def) ~= nil and string.match(def, "^'") == nil then
				-- an unquoted number (DEFAULT 12345) is stored as its text: measure exactly that text in SQL (the catalog
				-- may show a long fixed-point form, e.g. for 1.5E300); 2 characters of margin only if that fails
				local okl, rl = pquery('select length(cast(('..def..') as varchar(2000000))) as l')
				if okl and #rl == 1 and num(rl[1][1]) ~= nil then deflen = num(rl[1][1]) else deflen = #def + 2 end
			elseif deflen == nil and (string.upper(def) == 'TRUE' or string.upper(def) == 'FALSE') then
				deflen = 5                                   -- a BOOLEAN literal is stored as TRUE / FALSE
			end
		end

		if current_maxsize <= 3 then
			message_action = 'Keep '..src..' (n <= 3, left untouched)'
		elseif deflen == nil then
			message_action = 'Keep '..src..' (non-literal DEFAULT '..def..' - not changed)'
		else
			local dOk, dColumns = pquery([[
				select count(::col_name) as values_in_column,
				       coalesce(max(length(::col_name)), 0) as max_length
				from ::curr_schema.::curr_table
			]], {curr_schema=scm, curr_table=tbl, col_name=col})
			if not dOk then
				message_action = 'Keep '..src..' (check failed: '..dColumns.error_message..')'
			else
			local not_null_count, max_length = num(dColumns[1][1]), num(dColumns[1][2])
			if deflen > max_length then max_length = deflen end      -- the target must also hold the DEFAULT
			local incl = ''
			if def ~= nil then incl = ' (incl. DEFAULT '..def..')' end
			tgt = {fam='VC', n=current_maxsize, cs=charset, maxlen=max_length, empty=(not_null_count == 0), hasdef=(def ~= nil)}
			if not_null_count == 0 then
				message_action = 'Keep '..src..' (empty)'
			elseif estimate_optimal_varchar_length(max_length) < current_maxsize then
				local change_to  = estimate_optimal_varchar_length(max_length)
				query_to_execute = "ALTER TABLE "..scm.."."..tbl.." MODIFY ("..col.." VARCHAR("..change_to..") "..charset..");"
				modify_column    = true
				message_action   = src..' --> VARCHAR('..change_to..') '..charset..', max length: '..max_length..incl
			else
				message_action = 'Keep '..src..', max length: '..max_length..incl
			end
			end
		end
		result_table[#result_table+1] = {sch=res[i][1], tab=res[i][2], col=res[i][3], src=src,
			message=message_action, query=query_to_execute, modify=modify_column, tgt=tgt}
	end
	return result_table
end

------------------------------------------------------------------------------------------------------
-- Helper functions
------------------------------------------------------------------------------------------------------

-- Appends all rows of t2 to t1 and returns t1.
function merge_tables(t1, t2)
	for i = 1, #t2 do t1[#t1+1] = t2[i] end
	return t1
end

-- Returns the maximal length found in one column of the (positional) result table (bytes, i.e. >= characters).
function getMaxLengthForColumn(input_table, column_number)
	local length = 1
	for i = 1, #input_table do
		local v = input_table[i][column_number]
		if v ~= nil and #v > length then length = #v end
	end
	return math.min(length, 2000000)
end

-- Executes the SQL in one column of the positional table and logs the outcome in another column.
-- ALL-OR-NOTHING: the analysis and all statements run in ONE transaction (pending work of the session was
-- committed before the analysis). At the first error everything executed so far (incl. dropped FOREIGN KEYs)
-- is rolled back and the remaining statements are not executed; a fully successful run is committed.
-- Comment-only rows (section dividers starting with '--') and empty rows are skipped, NOT executed.
function execute_sql_column(input_table, sql_col_number, log_col_number)
	local executed = {}
	for i = 1, #input_table do
		local sql_stmt = input_table[i][sql_col_number]
		if sql_stmt ~= nil and sql_stmt ~= '' and string.sub(sql_stmt, 1, 2) ~= '--' then
			local sql_suc, sql_res = pquery(sql_stmt)
			if sql_suc then
				input_table[i][log_col_number] = 'true'
				executed[#executed+1] = i
			else
				input_table[i][log_col_number] = 'ERROR: ' .. sql_res.error_message .. ' - ALL changes of this run were rolled back'
				pquery([[rollback]])
				for _, j in ipairs(executed) do
					input_table[j][log_col_number] = 'rolled back (a later statement failed)'
				end
				for k = i + 1, #input_table do
					local s = input_table[k][sql_col_number]
					if s ~= nil and s ~= '' and string.sub(s, 1, 2) ~= '--' then
						input_table[k][log_col_number] = 'not executed (stopped at the first error)'
					else
						input_table[k][log_col_number] = ''
					end
				end
				return input_table
			end
		else
			input_table[i][log_col_number] = ''   -- nothing to execute (a "Keep" row or a section divider)
		end
	end
	query([[commit]])   -- durable regardless of the client's autocommit setting
	return input_table
end

-----------------------------END OF FUNCTIONS, BEGINNING OF ACTUAL SCRIPT-----------------------------

	-- every flag is TRUE only when TRUE is passed: SQL NULL (truthy in Lua) counts as FALSE
	local do_double, do_integer, do_decimal = (convert_double == true), (convert_integer == true), (convert_decimal == true)
	local reduce_scale = (reduce_decimal_scale == true)
	local do_timestamp, do_varchar = (convert_timestamp == true), (convert_varchar == true)
	local log_all, apply = (log_for_all_columns == true), (apply_conversion == true)
	SF_OP, SF_VAL = name_filter(schema_name)
	TF_OP, TF_VAL = name_filter(table_name)

	-- Apply mode: commit pending work of the session BEFORE the analysis, so that the analysis and every
	-- ALTER statement run in ONE transaction. A concurrent change to an analysed table then ends in a
	-- transaction collision (reported + full rollback) instead of being converted without a check.
	if apply then query([[commit]]) end

	-- 1) Collect every inspected column of the ENABLED types (records, not yet filtered by log_for_all_columns)
	local analyzed = {}
	if do_double    then analyzed = merge_tables(analyzed, convert_double_to_decimal(schema_name, table_name)) end
	if do_integer   then analyzed = merge_tables(analyzed, convert_integer_to_smaller_integer(schema_name, table_name)) end
	if do_decimal   then analyzed = merge_tables(analyzed, convert_decimal_with_scale_to_smaller_decimal(schema_name, table_name, reduce_scale)) end
	if do_timestamp then analyzed = merge_tables(analyzed, convert_timestamp_to_date(schema_name, table_name)) end
	if do_varchar   then analyzed = merge_tables(analyzed, convert_varchar_to_smaller_varchar(schema_name, table_name)) end

	-- 2) FOREIGN KEY handling (only when FKs touch the analyzed tables; otherwise no effect)
	local function nodek(s, t, c) return s .. string.char(1) .. t .. string.char(1) .. c end
	local acol = {}
	for i = 1, #analyzed do acol[nodek(analyzed[i].sch, analyzed[i].tab, analyzed[i].col)] = analyzed[i] end

	-- all FKs: key groups are built completely, also across schemas and over several hops. EXA_DBA_* first, so
	-- that FKs on tables the current user cannot see are known too; EXA_ALL_* when DBA views are not readable.
	local fk_sql = [[
		SELECT cc.constraint_schema, cc.constraint_table, cc.constraint_name, cc.ordinal_position,
		       cc.column_name, cc.referenced_schema, cc.referenced_table, cc.referenced_column,
		       c.constraint_enabled
		FROM   sys.exa_dba_constraint_columns cc
		JOIN   sys.exa_dba_constraints c
		       ON  c.constraint_schema = cc.constraint_schema
		       AND c.constraint_table  = cc.constraint_table
		       AND c.constraint_name   = cc.constraint_name
		WHERE  cc.constraint_type = 'FOREIGN KEY'
		ORDER  BY cc.constraint_schema, cc.constraint_table, cc.constraint_name, cc.ordinal_position
	]]
	local fk_suc, fk = pquery(fk_sql)
	if not fk_suc then fk_suc, fk = pquery((string.gsub(fk_sql, 'sys%.exa_dba_', 'sys.exa_all_'))) end
	-- without the FK catalog the key groups would be silently incomplete (MODIFYs on key columns without the
	-- needed DROP / RE-ADD FOREIGN KEY), e.g. when QUERY_TIMEOUT is reached - stop instead
	if not fk_suc then
		error('convert_datatypes stopped - the FOREIGN KEY catalog could not be read ('..fk.error_message..') - no column was changed')
	end

	local drop_rows, readd_rows = {}, {}

	if fk_suc and #fk > 0 then
		local parent, nodedisp = {}, {}
		local function find(x)
			if parent[x] == nil then parent[x] = x end
			while parent[x] ~= x do parent[x] = parent[parent[x]]; x = parent[x] end
			return x
		end
		local function union(a, b) local ra, rb = find(a), find(b); if ra ~= rb then parent[ra] = rb end end
		local fkdef, fkorder = {}, {}
		for i = 1, #fk do
			local cs, ct, cn = fk[i][1], fk[i][2], fk[i][3]
			local cc         = fk[i][5]
			local rs, rt, rc = fk[i][6], fk[i][7], fk[i][8]
			local cnode, rnode = nodek(cs, ct, cc), nodek(rs, rt, rc)
			nodedisp[cnode] = quote(cs)..'.'..quote(ct); nodedisp[rnode] = quote(rs)..'.'..quote(rt)
			union(cnode, rnode)
			local key = nodek(cs, ct, cn)
			if fkdef[key] == nil then
				fkdef[key] = {csch=cs, ctab=ct, cname=cn, cols={}, rsch=rs, rtab=rt, rcols={}, enabled=fk[i][9]}
				fkorder[#fkorder+1] = key
			end
			fkdef[key].cols[#fkdef[key].cols+1]   = cc
			fkdef[key].rcols[#fkdef[key].rcols+1] = rc
		end

		local groups = {}
		for nkey in pairs(parent) do
			local root = find(nkey)
			if groups[root] == nil then groups[root] = {} end
			groups[root][#groups[root]+1] = nkey
		end

		local converting = {}
		for root, nodes in pairs(groups) do
			local members, missing = {}, {}
			for _, nkey in ipairs(nodes) do
				if acol[nkey] ~= nil then members[#members+1] = acol[nkey] else missing[#missing+1] = nkey end
			end
			table.sort(members, function(x, y)      -- deterministic order (pairs() order is random)
				if x.sch ~= y.sch then return x.sch < y.sch end
				if x.tab ~= y.tab then return x.tab < y.tab end
				return x.col < y.col
			end)
			if #members > 0 then
				if #missing > 0 then
					local seen, rel = {}, {}
					for _, nkey in ipairs(missing) do
						local d = nodedisp[nkey] or '(unknown table)'
						if not seen[d] then seen[d] = true; rel[#rel+1] = d end
					end
					table.sort(rel)
					local relstr = table.concat(rel, ', ')
					local gt = group_target(members, reduce_scale)
					local undecided = (render_target(gt) ~= nil) or (gt.why == 'all key columns are empty')
					for _, a in ipairs(members) do
						if undecided then
							-- the in-filter part would convert (or is empty): only the whole group can be decided
							a.message = 'Keep '..a.src..' (FK key group also contains '..relstr..
							            ' outside the current filter or not visible to the current user - a key group is only checked'..
							            ' and converted as a whole; re-run with a filter that includes them)'
							a.notice  = true    -- actionable: shown even with log_for_all_columns = false
						elseif own_keep(a) then
							a.message = a.message..' [the FK key group also reaches '..relstr..']'
						else
							a.message = 'Keep '..a.src..' (FK key group: '..group_keep_reason(gt)..'; the group also reaches '..relstr..')'
						end
						a.query = ''; a.modify = false; a.scale_reduced = false
					end
				else
					local gt = group_target(members, reduce_scale)
					if gt.fam == 'DBL' then
						-- every member must convert exactly to the COMMON target (it may differ from its own one)
						for _, a in ipairs(members) do
							if not double_roundtrip_ok(quote(a.sch), quote(a.tab), quote(a.col), a.tgt.defsql, gt.p, gt.s)
							   or not double_default_ok(a.tgt.defsql, gt.p, gt.s) then
								gt = {fam='KEEP', why='key column '..quote(a.sch)..'.'..quote(a.tab)..'.'..quote(a.col)..
								      ' is not exactly representable as the common type DECIMAL('..gt.p..', '..gt.s..')'}
								break
							end
						end
					end
					local ttype = render_target(gt)
					if ttype ~= nil then
						for _, a in ipairs(members) do
							if not quoted_default_ok(a.tgt.def, ttype) then
								gt = {fam='KEEP', why='the DEFAULT '..a.tgt.def..' of key column '..quote(a.sch)..'.'..quote(a.tab)..'.'..quote(a.col)..
								      ' does not convert to '..ttype..' with the session NLS settings'}
								ttype = nil
								break
							end
						end
					end
					if ttype ~= nil then
						converting[root] = true
						for _, a in ipairs(members) do
							a.query   = "ALTER TABLE "..quote(a.sch).."."..quote(a.tab).." MODIFY ("..quote(a.col).." "..ttype..");"
							a.modify  = true
							a.scale_reduced = (gt.reduced == true)
							local tag = ' [FK key group - harmonized]'
							if a.scale_reduced then tag = ' [SCALE REDUCED]'..tag end
							a.message = a.src..' --> '..ttype..tag
							if gt.facts ~= nil then
								a.message = a.message..', group '..gt.facts
								for _, m in ipairs(members) do
									if m.tgt.hasdef then a.message = a.message..' (incl. DEFAULT values)'; break end
								end
							end
						end
					else
						for _, a in ipairs(members) do
							if not own_keep(a) then   -- a column kept for its own reason keeps that reason
								a.message = 'Keep '..a.src..' (FK key group: '..group_keep_reason(gt)..' - not changed)'
							end
							a.query = ''; a.modify = false; a.scale_reduced = false
						end
					end
				end
			end
		end

		for _, key in ipairs(fkorder) do
			local d = fkdef[key]
			local touches = false
			for _, cc in ipairs(d.cols) do
				if converting[find(nodek(d.csch, d.ctab, cc))] then touches = true; break end
			end
			if touches then
				local en    = d.enabled
				local state = (en == false or en == 'FALSE' or en == 'false' or en == 0 or en == '0') and 'DISABLE' or 'ENABLE'
				local ccols, rcols = {}, {}
				for _, c in ipairs(d.cols)  do ccols[#ccols+1]  = quote(c) end
				for _, c in ipairs(d.rcols) do rcols[#rcols+1] = quote(c) end
				drop_rows[#drop_rows+1] = {sch=d.csch, tab=d.ctab, col='', message='drop foreign key '..d.cname,
					query='ALTER TABLE '..quote(d.csch)..'.'..quote(d.ctab)..' DROP CONSTRAINT '..quote(d.cname)..';'}
				readd_rows[#readd_rows+1] = {sch=d.csch, tab=d.ctab, col='', message='re-add foreign key '..d.cname..' ('..state..')',
					query='ALTER TABLE '..quote(d.csch)..'.'..quote(d.ctab)..' ADD CONSTRAINT '..quote(d.cname)..
					      ' FOREIGN KEY ('..table.concat(ccols, ', ')..') REFERENCES '..quote(d.rsch)..'.'..quote(d.rtab)..
					      ' ('..table.concat(rcols, ', ')..') '..state..';'}
			end
		end
	end

	-- 3) Build the column rows honoring log_for_all_columns, sorted by schema/table/column;
	--    scale reductions are collected separately so they can be reviewed in their own section
	local normal_recs, scale_recs = {}, {}
	for i = 1, #analyzed do
		local a = analyzed[i]
		if a.modify and a.scale_reduced then
			scale_recs[#scale_recs+1] = a
		elseif a.modify or a.notice or log_all or string.find(a.message or '', '(check failed: ', 1, true) then
			normal_recs[#normal_recs+1] = a
		end
	end
	local function by_name(a, b)
		if a.sch ~= b.sch then return a.sch < b.sch end
		if a.tab ~= b.tab then return a.tab < b.tab end
		return a.col < b.col
	end
	table.sort(normal_recs, by_name)
	table.sort(scale_recs, by_name)

	-- 4) Assemble the final ordered list of records (DROP FKs first, MODIFYs, SCALE REDUCTIONS, RE-ADD FKs last)
	local ordered = {}
	local function add_column_rows()
		for _, x in ipairs(normal_recs) do ordered[#ordered+1] = x end
		if #scale_recs > 0 then
			ordered[#ordered+1] = {sch='', tab='', col='', message='',
				query='-- ### SCALE REDUCTIONS - REVIEW FIRST. After these, every later INSERT / IMPORT / MERGE / UPDATE value with more decimals than the new scale is ROUNDED SILENTLY ###'}
			for _, x in ipairs(scale_recs) do ordered[#ordered+1] = x end
		end
	end
	if #drop_rows > 0 then
		ordered[#ordered+1] = {sch='', tab='', col='', message='', query='-- ### DROP FOREIGN KEYS - run these FIRST (before the column changes) ###'}
		for _, x in ipairs(drop_rows)  do ordered[#ordered+1] = x end
		ordered[#ordered+1] = {sch='', tab='', col='', message='', query='-- ### COLUMN TYPE CHANGES ###'}
		add_column_rows()
		ordered[#ordered+1] = {sch='', tab='', col='', message='', query='-- ### RE-ADD FOREIGN KEYS - run these LAST (after the column changes) ###'}
		for _, x in ipairs(readd_rows) do ordered[#ordered+1] = x end
	else
		add_column_rows()
	end

	-- 5) Turn records into positional rows (add a success slot when applying)
	local overall_res = {}
	for i = 1, #ordered do
		local r = ordered[i]
		if apply then
			overall_res[#overall_res+1] = {r.sch, r.tab, r.col, r.message, r.query, ''}
		else
			overall_res[#overall_res+1] = {r.sch, r.tab, r.col, r.message, r.query}
		end
	end

	-- 6) Apply (in the assembled order: DROP -> MODIFY -> RE-ADD; all-or-nothing); section dividers are skipped
	if apply then
		overall_res = execute_sql_column(overall_res, 5, 6)
	end

	-- 7) Friendly message when nothing matched
	if #overall_res == 0 then
		local empty_msg
		if log_all then
			empty_msg = 'No matching columns found (check the filters and the convert_* switches).'
		else
			empty_msg = 'No columns found that need optimization.'
		end
		if apply then overall_res[1] = {'', '', '', empty_msg, '', ''}
		else                     overall_res[1] = {'', '', '', empty_msg, ''} end
	end

	-- 8) Size the output columns and return (order is intentional - do NOT re-sort).
	--    VARCHAR (not CHAR, which is capped at 2000) so long statements such as composite FK re-adds fit.
	local length_schema      = getMaxLengthForColumn(overall_res, 1)
	local length_table       = getMaxLengthForColumn(overall_res, 2)
	local length_column      = getMaxLengthForColumn(overall_res, 3)
	local length_conversions = getMaxLengthForColumn(overall_res, 4)
	local length_query       = getMaxLengthForColumn(overall_res, 5)
	if apply then
		local length_success = getMaxLengthForColumn(overall_res, 6)
		exit(overall_res, "schema_name varchar("..length_schema.."), table_name varchar("..length_table.."), column_name varchar("..length_column.."), conversion varchar("..length_conversions.."), query_text varchar("..length_query.."), success varchar("..length_success..")")
	else
		exit(overall_res, "schema_name varchar("..length_schema.."), table_name varchar("..length_table.."), column_name varchar("..length_column.."), conversion varchar("..length_conversions.."), query_text varchar("..length_query..")")
	end
/


-- ====================================================================================
-- !!! ATTENTION: run with apply_conversion = false first and REVIEW the output.   !!!
-- !!! Only set apply_conversion = true when you are 100% sure, or - safer - copy   !!!
-- !!! the generated statements and execute them yourself, in the shown order.      !!!
-- ====================================================================================
-- If executed with 'false' --> Script only displays what changes would be made
execute script DATABASE_MIGRATION.CONVERT_DATATYPES(
'MY_SCHEMA',  --  schema_name:          SCHEMA name (exact) or SCHEMA_FILTER with % (LIKE pattern, '_' = any single character)
'%',          --  table_name:           TABLE name (exact) or TABLE_FILTER with % (LIKE pattern, '_' = any single character)
true,         --  convert_double:       true (recommended) - DOUBLE -> smallest fitting DECIMAL(p,0) / DECIMAL(p,s), only when provably lossless
true,         --  convert_integer:      true (recommended) - DECIMAL(p,0) -> DECIMAL(9,0) / DECIMAL(18,0)
true,         --  convert_decimal:      true (recommended) - DECIMAL(p,s) -> DECIMAL(9,s) / DECIMAL(18,s), scale kept
true,         --  reduce_decimal_scale: true (recommended) = ALSO reduce the scale losslessly when the values use fewer decimals,
              --                        e.g. DECIMAL(36,18) -> DECIMAL(9,2) (listed in the section SCALE REDUCTIONS - review it).
              --                        WARNING: afterwards later loads with more decimals are ROUNDED SILENTLY. Needs convert_decimal = true.
              --                        false = keep every scale.
true,         --  convert_timestamp:    true (recommended) - TIMESTAMP / TIMESTAMP WITH LOCAL TIME ZONE -> DATE (no time component, no time-of-day DEFAULT)
true,         --  convert_varchar:      true (recommended) - VARCHAR(n) -> smaller VARCHAR (same charset)
false,        --  log_for_all_columns:  false (recommended) = only report columns that change (plus failed checks and FK notes), true = report every inspected column with its 'Keep' reason
false         --  apply_conversion:     false (recommended) = only report; true = irreversibly apply (all-or-nothing, rollback at the first error)
);
