create schema if not exists database_migration;

/*
    postgresql_to_exasol.sql  -  generate the statements to migrate a PostgreSQL database to Exasol v8.

    Source: PostgreSQL 12 or newer (tested with 14, 15, 16, 17 and 18). An older source stops with a clear error.
    This script runs on the TARGET Exasol database, reads the SOURCE metadata through a JDBC connection and
    RETURNS the statements to recreate and load the source. It changes nothing itself - review the output and run
    it in the order returned, in ONE session (see TIME ZONE below), preferably with stop-on-error
    (EXAplus option -x; EXAplus continues after errors by default). In EXAplus run SET DEFINE OFF; first when the
    output contains the character & (a note says so), otherwise EXAplus treats &name as a substitution variable.

    OUTPUT (in this order): header and notes, CREATE SCHEMA, DROP TABLE IF EXISTS ... CASCADE CONSTRAINTS +
    CREATE TABLE, PRIMARY KEYs and FOREIGN KEYs (created DISABLED), PARTITION BY, COMMENTs, the TIME ZONE block,
    the IMPORTs, the CONSTRAINT STATE section, the optional DATA VALIDATION (CHECK_MIGRATION), the restore of the
    session time zone, and the optional commented VIEW review section (with MIGRATE_MATERIALIZED_VIEWS also the
    definitions of the migrated materialized views). Re-running the output replaces the target
    tables (DROP ... CASCADE CONSTRAINTS also drops foreign keys that OTHER Exasol tables hold on them).

    TIME ZONE (important): timestamptz is migrated losslessly as TIMESTAMP(p) WITH LOCAL TIME ZONE. The values are
    transferred as UTC. Exasol interprets incoming values in the SESSION time zone, so the output switches the
    session to TIME_ZONE = 'UTC' before the IMPORTs and restores the original zone at the end. Run the ALTER SESSION
    and the IMPORTs in the SAME session. Afterwards every session sees the values in its own time zone. The usable
    timestamptz range is 0001-01-02 .. 9999-12-30 UTC (values on the outermost day cannot be displayed in every
    session time zone); TEMPORAL_OUT_OF_RANGE applies to values outside it.

    DATA TYPE MAPPING (by pg_type category; built-in types are recognised in pg_catalog only, domains resolve to
    their base type, so every type is covered):
      smallint/integer/bigint/oid -> DECIMAL(5/10/19/10,0); numeric(p,s) -> DECIMAL(p,s) (negative scale ->
      DECIMAL(p-s,0), scale > precision -> DECIMAL(s,s), p > 36 or unconstrained -> DECIMAL_OVERFLOW);
      real/double precision -> DOUBLE (a real value arrives as the double nearest its PostgreSQL text, e.g. 0.1);
      money -> DECIMAL(20,2); boolean -> BOOLEAN; char(n<=2000) -> CHAR(n) UTF8;
      char(n>2000)/varchar(n)/text/name -> VARCHAR UTF8; date -> DATE; timestamp(p) -> TIMESTAMP(p);
      timestamptz(p) -> TIMESTAMP(p) WITH LOCAL TIME ZONE; uuid -> CHAR(36); bytea -> base64 text (BINARY_HANDLING).
      Small documented differences: time / timetz -> VARCHAR (Exasol has no TIME type); interval -> VARCHAR or
      native INTERVAL DAY TO SECOND(3), i.e. millisecond precision (INTERVAL_HANDLING); enum -> VARCHAR(63); inet -> VARCHAR (output format);
      json/jsonb/xml, arrays, ranges, geometric, bit, tsvector, composite, extension types, ... -> VARCHAR (faithful
      text). numeric/real/double NaN and +-Infinity -> NULL (Exasol has no such values; DECIMAL_OVERFLOW='VARCHAR'
      columns keep the text 'NaN' / 'Infinity').
      Exasol stores an empty string as NULL: empty character, bytea and other text values become NULL. NOT NULL is
      therefore only kept on integer, money, date, timestamp and boolean targets - not on numeric (NaN becomes NULL)
      and not on date/timestamp under TEMPORAL_OUT_OF_RANGE='NULL'. Columns named LEVEL, ROWNUM, ROWID,
      CONNECT_BY_ISLEAF or CONNECT_BY_ISCYCLE (not allowed as Exasol column names) get a trailing underscore.
      Many PostgreSQL column names (DATE, TIME, YEAR, VALUE, USER, ...) are reserved words in Exasol and must be
      quoted in queries - the output lists them as notes.
      Hard limits (the IMPORT fails loudly rather than corrupting data): a value > 2,000,000 characters (unless
      TRUNCATE_LONG_STRINGS=true), a bytea > 1,500,000 bytes, a numeric value with more integer digits than its
      DECIMAL_OVERFLOW='CAP' column holds (numeric(p > 36, s) keeps min(p - s, 36) integer digits and rounds surplus
      fractional digits; numeric without precision becomes DECIMAL(36,18) and holds 18 integer digits), a date/timestamp outside the
      Exasol range under TEMPORAL_OUT_OF_RANGE='FAIL' (incl. infinity and BC values), a source value that is not
      valid UTF-8 (SQL_ASCII databases).

    PARALLEL IMPORT: from PostgreSQL 14 on, a table with at least PARALLEL_MIN_ROWS rows is read by up to
    PARALLEL_STATEMENTS parallel STATEMENT clauses, each reading a disjoint ctid block range (exact 1:1). Each
    STATEMENT is a separate PostgreSQL transaction - the source must not be written during the load. 1 = off.
    AUTO = half the vCPUs of one Exasol node (VCPU/NODES/2), even, 4..64 - the rule of snowflake_to_exasol.sql -,
    at most half of the free PostgreSQL connections. Each stream is one PostgreSQL backend (about one CPU core) and
    needs temporary memory in Exasol: for a weak PostgreSQL server set PARALLEL_STATEMENTS to at most about twice its
    CPU cores; on Exasol nodes with little memory use fewer streams for very large tables. PARALLEL_MIN_ROWS
    (default 1,000,000): every stream starts with a delay of about one second, so smaller tables load faster with
    one STATEMENT (measured on a single node: 200,000 rows 5 s with 1 vs 10 s with 8 STATEMENTs; 1,000,000 rows 20 s vs 12 s with 4).
    The row estimate comes from the PostgreSQL statistics (reltuples scaled to the current table size); a table
    without statistics (never analyzed) is estimated at one row per 80 bytes. Long-running STATEMENTs keep their
    source transactions open, which holds back VACUUM on the source for that time.
    Every STATEMENT repeats the full select list, and one generated row holds at most 2,000,000 characters: for
    a very wide table the number of STATEMENTs is reduced automatically so that its IMPORT fits into one row
    (a PARALLEL note names the table). Even in the worst case at least 7 STATEMENTs fit: a STATEMENT is accepted only
    below 131,072 bytes, its quoted form is at most twice as long, and PostgreSQL allows at most 1,600 columns.
    Typical wide tables keep far more; all other tables keep the full number.

    CONSTRAINTS: PK/FK are always created DISABLED; a final CONSTRAINT STATE section enables them according to
    CONSTRAINT_STATE. A foreign key is migrated when its parent table is migrated and it references the parent
    primary key; a child column type is widened to the parent key type when that is lossless.

    PARTITIONING / INHERITANCE: a single-column declarative partition key on a partitionable Exasol type becomes
    ALTER TABLE ... PARTITION BY (GENERATE_PARTITION_BY); partitions are read through their parent. Legacy
    INHERITS hierarchies are read with FROM ONLY (every table keeps exactly its own rows).

    Not migrated (out of scope, listed as notes where relevant): indexes, UNIQUE/CHECK/EXCLUSION constraints,
    sequences, identity behaviour (identity columns carry their values), generated columns (carried as values),
    functions/procedures/triggers, foreign tables, extension-owned tables, row-level security policies (use a
    migration user with BYPASSRLS), users/roles/privileges. Materialized views only with MIGRATE_MATERIALIZED_VIEWS.
    TimescaleDB hypertables (inheritance parent + chunk tables in _timescaledb_internal) are not migrated as one table.
    A TABLE_FILTER that names a single partition matches nothing: partitions are migrated through their parent.

    KNOWN LIMITS: dates/timestamps between 1582-10-05 and 1582-10-14 (Julian/Gregorian gap) arrive shifted by the
    JDBC transfer; citext columns and columns with a nondeterministic collation compare case-insensitively in
    PostgreSQL but binary in Exasol (a note names them); json (not jsonb) values with invalid unicode escapes count
    as INVALID_JSON in Exasol. CHECK_MIGRATION computes count(distinct) per column on the source (cost on large
    tables) and needs CREATE TABLE / INSERT / DELETE in the script schema for non-DBA users; to re-run only the CHECK
    section, run it together with the final restore row. With INTERVAL_HANDLING='INTERVAL' an interval beyond
    999,999,999 days follows TEMPORAL_OUT_OF_RANGE like infinity. A numeric primary key that holds NaN loads (as NULL)
    but cannot be enabled. Exasol DOUBLE rejects the
    largest IEEE values (about 1.7977e308). A primary key on a text column that contains '' cannot be enabled ('' is
    NULL in Exasol), and Exasol also refuses to enable a primary key whose values differ only by trailing blanks
    ('a' / 'a '), although = and DISTINCT treat them as different. <table>_MIG_CHK tables of older releases in the target schemas are not removed.
*/
--/
create or replace script database_migration.POSTGRESQL_TO_EXASOL(
  CONNECTION_NAME               -- name of the JDBC connection inside Exasol, e.g. POSTGRESQL_JDBC
  ,IDENTIFIER_CASE_INSENSITIVE  -- true (recommended) => fold all identifiers to UPPER case; false => keep them as in PostgreSQL
  ,SCHEMA_FILTER                -- LIKE pattern for the source schemas, '%' = all ('_' and '%' are wildcards; system schemas are always excluded)
  ,TABLE_FILTER                 -- LIKE pattern for the tables/views, '%' = all
  ,TARGET_SCHEMA                -- target schema on Exasol; '' = use the source schema name
  ,PARALLEL_STATEMENTS          -- 'AUTO' (recommended: Exasol VCPU/NODES/2, even, 4..64, at most half of the free PostgreSQL connections), a number >= 1, or 1 = no parallel reading (one STATEMENT per table)
  ,PARALLEL_MIN_ROWS            -- tables with fewer (estimated) rows are read with one STATEMENT (default 1000000); 0 = always split
  ,CONSTRAINT_STATE             -- 'FORCE_DISABLE' (recommended), 'SET_AS_SOURCE' or 'FORCE_ENABLE'
  ,GENERATE_COMMENTS            -- true/false: migrate schema, table and column comments
  ,GENERATE_VIEWS               -- true/false: list the source views as a commented manual-review section
  ,MIGRATE_MATERIALIZED_VIEWS   -- false (default) => materialized views are only listed for review; true => migrate them as tables with their data
  ,GENERATE_PARTITION_BY        -- true/false: best-effort PARTITION BY from a single-column PostgreSQL partition key
  ,BINARY_HANDLING              -- 'BASE64' (recommended; bytea as base64 text) or 'SKIP' (load NULL)
  ,DECIMAL_OVERFLOW             -- 'CAP' (recommended), 'DOUBLE' or 'VARCHAR' for numeric with precision > 36 or without precision
  ,TRUNCATE_LONG_STRINGS        -- false (recommended): a value > 2,000,000 characters makes the IMPORT fail; true: cut it to 2,000,000 characters
  ,INTERVAL_HANDLING            -- 'VARCHAR' (recommended; lossless text) or 'INTERVAL' (native INTERVAL DAY TO SECOND(3) - millisecond precision, best-effort)
  ,TEMPORAL_OUT_OF_RANGE        -- 'FAIL' (recommended), 'NULL' or 'CLAMP' for date/timestamp values outside the Exasol range
  ,CHECK_MIGRATION              -- true/false: also generate a source-vs-target data validation (run after the IMPORTs)
) RETURNS TABLE
AS

-------------------------------------------------------------------------------------------------------------
-- helpers
-------------------------------------------------------------------------------------------------------------
local NL = string.char(10)
local CR = string.char(13)
local SQ = string.char(39)          -- single quote
local DQ = string.char(34)          -- double quote

local function fail(msg) error('POSTGRESQL_TO_EXASOL: ' .. msg) end
local function isnull(v) return v == nil or v == null end
local function str(v) if isnull(v) then return nil end return tostring(v) end
local function int(v) if isnull(v) then return nil end return math.floor(tonumber(v)) end
local function istr(n) return string.format('%d', math.floor(n)) end
local function trim(s) return (s:gsub('^%s+', ''):gsub('%s+$', '')) end
local function sl(s) return SQ .. (s:gsub(SQ, SQ .. SQ)) .. SQ end                 -- SQL string literal
local function qi(s) return DQ .. (s:gsub(DQ, DQ .. DQ)) .. DQ end                 -- quoted identifier (Exasol and PostgreSQL)
local function oneline(s) return (s:gsub('%c', ' ')) end                           -- names inside comment rows
local UTF8_CONT = '[' .. string.char(128) .. '-' .. string.char(191) .. ']'
local function ulen(s) return #s - select(2, s:gsub(UTF8_CONT, '')) end            -- length in characters (UTF-8)
local function usub(s, n)                                                          -- first n characters (UTF-8)
	local count = 0
	for i = 1, #s do
		local b = s:byte(i)
		if b < 128 or b >= 192 then
			count = count + 1
			if count > n then return s:sub(1, i - 1) end
		end
	end
	return s
end
local C0 = '[' .. string.char(1) .. '-' .. string.char(8) .. string.char(11, 12) .. string.char(14) .. '-' .. string.char(31) .. string.char(127) .. ']'
local function cmt(s)                                                              -- multi-line text as comment lines
	s = s:gsub(C0, ' ')                                                            -- other control characters break EXAplus file mode
	s = s:gsub(CR .. NL, NL):gsub(CR, NL)
	return (s:gsub(NL, NL .. '-- '))
end

-------------------------------------------------------------------------------------------------------------
-- parameter validation (invalid values fail loudly)
-------------------------------------------------------------------------------------------------------------
local function p_bool(v, name, default)
	if isnull(v) then return default end
	if type(v) == 'boolean' then return v end
	if type(v) == 'string' then
		local u = trim(v):upper()
		if u == '' then return default end
		if u == 'TRUE' then return true end
		if u == 'FALSE' then return false end
	end
	fail('invalid value ' .. tostring(v) .. ' for ' .. name .. ' - valid: true, false')
end
local function p_opt(v, name, valid)
	if isnull(v) then return valid[1] end
	local u = trim(tostring(v)):upper()
	if u == '' then return valid[1] end
	for _, x in ipairs(valid) do if u == x then return u end end
	fail('invalid value ' .. tostring(v) .. ' for ' .. name .. ' - valid: ' .. table.concat(valid, ', '))
end

local CONN = str(CONNECTION_NAME)
if CONN == nil or trim(CONN) == '' then fail('invalid value (empty) for CONNECTION_NAME - valid: the name of an existing JDBC connection') end
CONN = trim(CONN)
if not (CONN:match('^[%a_][%w_]*$') or (CONN:match('^' .. DQ .. '[^' .. DQ .. ']+' .. DQ .. '$'))) then
	fail('invalid value ' .. CONN .. ' for CONNECTION_NAME - valid: the name of an existing CONNECTION object (an identifier, not a JDBC URL)')
end
local ICI      = p_bool(IDENTIFIER_CASE_INSENSITIVE, 'IDENTIFIER_CASE_INSENSITIVE', true)
local SF       = str(SCHEMA_FILTER);  if SF == nil or SF == '' then SF = '%' end
local TF       = str(TABLE_FILTER);   if TF == nil or TF == '' then TF = '%' end
local TGT      = str(TARGET_SCHEMA);  if TGT ~= nil then TGT = trim(TGT); if TGT == '' then TGT = nil end end
local TGT_UP   = TGT and query('select upper(' .. sl(TGT) .. ') from sys.dual')[1][1] or nil
local CSTATE   = p_opt(CONSTRAINT_STATE, 'CONSTRAINT_STATE', {'FORCE_DISABLE', 'SET_AS_SOURCE', 'FORCE_ENABLE'})
local G_COMM   = p_bool(GENERATE_COMMENTS, 'GENERATE_COMMENTS', true)
local G_VIEWS  = p_bool(GENERATE_VIEWS, 'GENERATE_VIEWS', true)
local G_MV     = p_bool(MIGRATE_MATERIALIZED_VIEWS, 'MIGRATE_MATERIALIZED_VIEWS', false)
local G_PART   = p_bool(GENERATE_PARTITION_BY, 'GENERATE_PARTITION_BY', true)
local BINMODE  = p_opt(BINARY_HANDLING, 'BINARY_HANDLING', {'BASE64', 'SKIP'})
local DECOF    = p_opt(DECIMAL_OVERFLOW, 'DECIMAL_OVERFLOW', {'CAP', 'DOUBLE', 'VARCHAR'})
local TRUNC    = p_bool(TRUNCATE_LONG_STRINGS, 'TRUNCATE_LONG_STRINGS', false)
local IVMODE   = p_opt(INTERVAL_HANDLING, 'INTERVAL_HANDLING', {'VARCHAR', 'INTERVAL'})
local OORMODE  = p_opt(TEMPORAL_OUT_OF_RANGE, 'TEMPORAL_OUT_OF_RANGE', {'FAIL', 'NULL', 'CLAMP'})
local G_CHECK  = p_bool(CHECK_MIGRATION, 'CHECK_MIGRATION', false)

-- whole numbers only (no '0x10', '1e1'), at most 2,147,483,647
local function p_int(v)
	local n
	if type(v) == 'string' then
		local t = trim(v)
		if not (t:match('^%d+$') or t:match('^%d+%.0*$')) then return nil end
		n = tonumber(t)
	else
		n = tonumber(v)
	end
	if n == nil or n ~= math.floor(n) or n > 2147483647 then return nil end
	return n
end
local PS_AUTO, PS_FIX = false, 1
do
	local v = PARALLEL_STATEMENTS
	if isnull(v) or (type(v) == 'string' and (trim(v) == '' or trim(v):upper() == 'AUTO')) then
		PS_AUTO = true
	else
		local n = p_int(v)
		if n == nil or n < 1 or n ~= math.floor(n) then
			fail('invalid value ' .. tostring(v) .. ' for PARALLEL_STATEMENTS - valid: AUTO or an integer from 1 to 2147483647')
		end
		PS_FIX = math.floor(n)
	end
end
local PMIN = 1000000
do
	local v = PARALLEL_MIN_ROWS
	if not (isnull(v) or (type(v) == 'string' and trim(v) == '')) then
		local n = p_int(v)
		if n == nil or n < 0 or n ~= math.floor(n) then
			fail('invalid value ' .. tostring(v) .. ' for PARALLEL_MIN_ROWS - valid: an integer from 0 to 2147483647')
		end
		PMIN = math.floor(n)
	end
end

-------------------------------------------------------------------------------------------------------------
-- remote (PostgreSQL) access: every statement runs with pinned session settings
-------------------------------------------------------------------------------------------------------------
-- zero output columns: no source column name can collide with it, and column references need no qualification
local PIN = [[(select from (select set_config('TimeZone', 'UTC', true), set_config('IntervalStyle', 'postgres', true), set_config('bytea_output', 'hex', true), set_config('search_path', 'pg_catalog', true)) as "__pin0") as "__pin"]]
local function remote(pgsql, exa_prefix, exa_suffix)
	-- exa_prefix: Exasol select list around the imported columns, e.g. 't.*, upper(t."relname")'
	local q = 'select ' .. (exa_prefix or 't.*') .. ' from (import from jdbc at ' .. CONN .. ' statement ' .. sl(pgsql) .. ') t' .. (exa_suffix or '')
	return query(q)
end
local pg_sf = sl(SF)
local pg_tf = sl(TF)
local BASEFILTER = " left(n.nspname, 3) <> 'pg_' and n.nspname <> 'information_schema' and n.nspname like " .. pg_sf .. " and c.relname like " .. pg_tf .. " "
local EXTEXCL = " not exists (select 1 from pg_depend d where d.classid = 'pg_class'::regclass and d.objid = c.oid and d.deptype = 'e') "
local RELFILTER = BASEFILTER .. ' and ' .. EXTEXCL

-- source environment + version check
-- a PostgreSQL older than 12 fails this query (pg_partition_root); then report the version clearly
local ok_env, env = pquery('select t.* from (import from jdbc at ' .. CONN .. ' statement ' .. sl([=[select current_setting('server_version_num')::int as vnum, current_setting('server_version') as ver,
	current_setting('server_encoding') as enc, current_setting('block_size')::int as bs,
	current_setting('max_connections')::int as maxc, current_setting('superuser_reserved_connections')::int as sures,
	coalesce(nullif(current_setting('reserved_connections', true), ''), '0')::int as res,
	(select count(*) from pg_stat_activity where pid <> pg_backend_pid() and backend_type = 'client backend')::int as used,
	(select count(*) from pg_namespace where left(nspname, 12) = '_timescaledb')::int as tsdb,
	(select string_agg(x.lbl, ', ') from (select regexp_replace(n.nspname || '.' || c.relname, '[[:cntrl:]]', ' ', 'g') as lbl
	   from pg_class c join pg_namespace n on n.oid = c.relnamespace
	  where c.relispartition and c.relkind in ('r', 'p') and]=] .. BASEFILTER .. [[
	    and not exists (select 1 from pg_class rc join pg_namespace rn on rn.oid = rc.relnamespace
	                     where rc.oid = pg_partition_root(c.oid) and rn.nspname like ]] .. pg_sf .. [[ and rc.relname like ]] .. pg_tf .. [[)
	  order by 1 limit 20) x) as lone_parts]]) .. ') t')
if not ok_env then
	local v = remote([[select current_setting('server_version_num')::int, current_setting('server_version')]])
	if int(v[1][1]) < 120000 then fail('PostgreSQL 12 or newer required (found ' .. str(v[1][2]) .. ')') end
	error(env.error_message or 'the source environment query failed')
end
local VNUM, VER, ENC, BS = int(env[1][1]), str(env[1][2]), str(env[1][3]), int(env[1][4])
if VNUM < 120000 then fail('PostgreSQL 12 or newer required (found ' .. VER .. ')') end
local HEADROOM = int(env[1][5]) - int(env[1][6]) - int(env[1][7]) - int(env[1][8])
local TSDB = int(env[1][9]) > 0
local LONE_PARTS = str(env[1][10])

-- Exasol environment
local SCRIPT_SCHEMA = exa.meta.script_schema
local SESSION_TZ = query('select sessiontimezone from sys.dual')[1][1]
local NODES, VCPU = nil, nil
do
	local ok, r = pquery([[select NODES, VCPU from EXA_STATISTICS.EXA_SYSTEM_EVENTS where EVENT_TYPE = 'STARTUP' order by MEASURE_TIME desc limit 1]])
	if ok and #r == 1 and not isnull(r[1][1]) and not isnull(r[1][2]) then NODES = int(r[1][1]); VCPU = int(r[1][2]) end
end
local RESERVED = {}
do
	local r = query([[select upper(KEYWORD) from EXA_SQL_KEYWORDS where RESERVED]])
	for i = 1, #r do RESERVED[r[i][1]] = true end
end
local FORBIDDEN_COL = {LEVEL = true, ROWNUM = true, ROWID = true, CONNECT_BY_ISLEAF = true, CONNECT_BY_ISCYCLE = true}

-- resolve PARALLEL_STATEMENTS
local PS, PS_NOTE
if VNUM < 140000 then
	PS = 1
	PS_NOTE = 'PostgreSQL ' .. VER .. ' has no TID range scan (needs 14+): every table is read with one STATEMENT'
elseif PS_AUTO then
	-- same rule as snowflake_to_exasol.sql: half the vCPUs of one Exasol node, even, 4..64
	local why
	if VCPU and NODES and NODES > 0 then
		PS = math.floor(VCPU / NODES / 2)
		if PS % 2 == 1 then PS = PS - 1 end
		PS = math.max(4, math.min(PS, 64))
		why = 'Exasol VCPU ' .. istr(VCPU) .. ' / nodes ' .. istr(NODES) .. ' / 2, even, 4..64'
	else
		PS = 4
		why = 'fallback; EXA_STATISTICS.EXA_SYSTEM_EVENTS not readable'
	end
	local cap = math.floor(HEADROOM / 2)
	if cap < PS then PS = math.max(1, cap); why = why .. '; capped to half of the PostgreSQL connection headroom ' .. istr(HEADROOM) end
	PS_NOTE = 'AUTO -> ' .. istr(PS) .. ' (' .. why .. ')'
else
	PS = PS_FIX
	PS_NOTE = 'fixed -> ' .. istr(PS)
end

-------------------------------------------------------------------------------------------------------------
-- output collection
-------------------------------------------------------------------------------------------------------------
local OUT = {}
local function add(s) OUT[#OUT + 1] = {s} end
local NOTES = {}
local function note(topic, text, warn)
	NOTES[#NOTES + 1] = (warn and '-- !!! ' or '-- NOTE ') .. topic .. ': ' .. cmt(text)
end

local function exa_id(raw, up) if ICI then return up else return raw end end

-------------------------------------------------------------------------------------------------------------
-- relations
-------------------------------------------------------------------------------------------------------------
local rel_q = [=[select c.oid::bigint as oid, n.nspname, c.relname, c.relkind::text as relkind, c.relrowsecurity, c.relispopulated,
	coalesce(c.reltuples, -1)::float8 as reltuples, c.relpages::bigint as relpages,
	(pg_relation_size(c.oid) / current_setting('block_size')::int)::bigint as blocks,
	(select left(string_agg(regexp_replace(quote_ident(cn.nspname) || '.' || quote_ident(ch.relname), '[[:cntrl:]]', ' ', 'g'), ', ' order by cn.nspname, ch.relname), 4000)
	   from pg_inherits i join pg_class ch on ch.oid = i.inhrelid join pg_namespace cn on cn.oid = ch.relnamespace
	  where i.inhparent = c.oid and c.relkind = 'r') as children,
	exists (select 1 from pg_extension e where c.oid = any(e.extconfig)) as extcfg,
	exists (select 1 from pg_inherits i join pg_class lc on lc.oid = i.inhrelid where i.inhparent = c.oid and lc.relkind = 'f') as foreign_part,
	coalesce((select am.amname::text from pg_am am where am.oid = c.relam), '') as am,
	exists (select 1 from pg_partition_tree(c.oid) l join pg_class lc on lc.oid = l.relid join pg_am am on am.oid = lc.relam
	        where l.isleaf and c.relkind = 'p' and am.amname <> 'heap') as nonheap_part
	from pg_class c join pg_namespace n on n.oid = c.relnamespace
	where c.relkind in ('r', 'p', 'm', 'f', 'v') and not c.relispartition and]=] .. BASEFILTER ..
	[[ and (exists (select 1 from pg_extension e where c.oid = any(e.extconfig)) or ]] .. EXTEXCL .. ')'
local rr = remote(rel_q, 't.*, upper(t."nspname"), upper(t."relname")')
local RELS, REL_BY_OID = {}, {}
for i = 1, #rr do
	local x = rr[i]
	local r = {oid = int(x[1]), nsp = x[2], rel = x[3], kind = x[4], rls = x[5], populated = x[6], reltuples = tonumber(x[7]),
	           relpages = int(x[8]), blocks = int(x[9]), children = str(x[10]), extcfg = x[11], foreign_part = x[12],
	           am = str(x[13]), nonheap_part = x[14], nsp_up = x[15], rel_up = x[16], cols = {}, colmap = {}}
	r.exa_schema = TGT and (ICI and TGT_UP or TGT) or exa_id(r.nsp, r.nsp_up)
	r.exa_table = exa_id(r.rel, r.rel_up)
	r.label = oneline(r.nsp) .. '.' .. oneline(r.rel)
	r.migrate = (r.kind == 'r' or r.kind == 'p' or (r.kind == 'm' and G_MV)) and not r.extcfg
	RELS[#RELS + 1] = r
	REL_BY_OID[r.oid] = r
end
table.sort(RELS, function(a, b) if a.exa_schema ~= b.exa_schema then return a.exa_schema < b.exa_schema end
	if a.exa_table ~= b.exa_table then return a.exa_table < b.exa_table end return a.nsp < b.nsp end)

-- target schema guard
do
	local done = {}
	for _, r in ipairs(RELS) do
		if r.migrate and not done[r.exa_schema] and RESERVED[r.exa_schema:upper()] then
			done[r.exa_schema] = true
			note('RESERVED NAME', 'the schema name ' .. oneline(r.exa_schema) .. ' is a reserved word in Exasol and must be quoted in queries', false)
		end
	end
end
for _, r in ipairs(RELS) do
	if r.migrate then
		local u = r.exa_schema:upper()
		if u == 'SYS' or u == 'EXA_STATISTICS' or u == 'EXA_SYSTEM' then
			fail('target schema ' .. r.exa_schema .. ' collides with an Exasol system schema - set TARGET_SCHEMA')
		end
	end
end

-- notes about relations that are not migrated or need attention
for _, r in ipairs(RELS) do
	if r.extcfg then note('EXTENSION CONFIG TABLE', 'table ' .. r.label .. ' belongs to an extension (configuration table); it is not migrated and may contain user rows - migrate them manually if needed', true) end
	if r.kind == 'f' then note('FOREIGN TABLE', 'foreign table ' .. r.label .. ' is not migrated', true) end
	if r.kind == 'm' and not G_MV then note('MATERIALIZED VIEW', 'materialized view ' .. r.label .. ' is not migrated (set MIGRATE_MATERIALIZED_VIEWS = true to migrate its content as a table)', false) end
	if r.migrate and r.rls then note('ROW LEVEL SECURITY', 'table ' .. r.label .. ' has row-level security - the IMPORT sees only the rows the connection user may read; use a user with BYPASSRLS or add options=-c row_security=off to the JDBC URL to make filtering fail loudly', true) end
	if r.migrate and r.kind == 'r' and r.children then note('INHERITANCE', 'table ' .. r.label .. ' has PostgreSQL inheritance children (' .. r.children .. '). In PostgreSQL a query on the parent also returns the children rows; Exasol has no table inheritance, so the Exasol table holds only its own rows. Create a UNION ALL view if needed.', false) end
	if r.migrate and r.kind == 'm' and not r.populated then note('MATERIALIZED VIEW', 'materialized view ' .. r.label .. ' is not populated (WITH NO DATA) - the table is created empty, no IMPORT', true) end
end
if ENC == 'SQL_ASCII' then note('SQL_ASCII', 'the source database encoding is SQL_ASCII - values that are not valid UTF-8 make the IMPORT of their table fail', true) end
if LONE_PARTS then note('PARTITION FILTER', 'SCHEMA_FILTER / TABLE_FILTER match partitions whose partitioned parent is not matched; partitions are migrated only through their parent - widen the filter to the parent: ' .. LONE_PARTS, true) end
for _, r in ipairs(RELS) do
	if PS > 1 and r.migrate and (((r.kind == 'r' or r.kind == 'm') and r.am ~= 'heap') or (r.kind == 'p' and r.nonheap_part)) then
		note('PARALLEL', 'table ' .. r.label .. ' uses a table access method other than heap and is read with one STATEMENT (ctid block ranges need heap)', false)
	end
end
if TSDB then note('TIMESCALEDB', 'TimescaleDB schemas exist - hypertables are inheritance parents with chunk tables in _timescaledb_internal and are not migrated as one table; review them manually', true) end

-- name collisions (two source relations -> one Exasol table)
do
	local seen = {}
	for _, r in ipairs(RELS) do
		if r.migrate then
			local k = r.exa_schema .. NL .. r.exa_table
			if seen[k] then
				seen[k].migrate = false; r.migrate = false
				note('NAME COLLISION', 'source tables ' .. seen[k].label .. ' and ' .. r.label .. ' map to the same Exasol table ' .. oneline(r.exa_schema) .. '.' .. oneline(r.exa_table) .. ' - neither is migrated; use IDENTIFIER_CASE_INSENSITIVE = false or separate TARGET_SCHEMA runs', true)
			else
				seen[k] = r
			end
		end
	end
end

-------------------------------------------------------------------------------------------------------------
-- columns
-------------------------------------------------------------------------------------------------------------
local col_q = [[with recursive dom(domain_oid, base_oid, dmod, dnotnull, ddefault) as (
		select t.oid, t.typbasetype, t.typtypmod, t.typnotnull, t.typdefault from pg_type t where t.typtype = 'd'
		union all
		select d.domain_oid, b.typbasetype, case when d.dmod <> -1 then d.dmod else b.typtypmod end, d.dnotnull or b.typnotnull, coalesce(d.ddefault, b.typdefault)
		from dom d join pg_type b on b.oid = d.base_oid where b.typtype = 'd'),
	dbase as (select d.* from dom d join pg_type bt on bt.oid = d.base_oid where bt.typtype <> 'd'),
	cols as (
	select c.oid::bigint as reloid, a.attname, a.attnum::int as attnum,
		coalesce(db.base_oid, a.atttypid) as eoid,
		case when db.base_oid is not null then case when db.dmod <> -1 then db.dmod else a.atttypmod end else a.atttypmod end as emod,
		a.attnotnull, coalesce(db.dnotnull, false) as dnotnull, db.ddefault,
		exists (select 1 from pg_constraint k where k.conrelid = a.attrelid and k.contype = 'n' and k.conkey[1] = a.attnum and not k.convalidated) as nn_notvalid,
		a.attidentity::text as identity, a.attgenerated::text as generated,
		pg_get_expr(ad.adbin, ad.adrelid) as defexpr,
		coalesce((select co.collisdeterministic from pg_collation co where co.oid = a.attcollation), true) as coll_det
	from pg_attribute a join pg_class c on c.oid = a.attrelid join pg_namespace n on n.oid = c.relnamespace
	left join dbase db on db.domain_oid = a.atttypid
	left join pg_attrdef ad on ad.adrelid = a.attrelid and ad.adnum = a.attnum
	where a.attnum > 0 and not a.attisdropped and c.relkind in ('r', 'p', 'm') and not c.relispartition and]] .. RELFILTER .. [[)
	select x.reloid, x.attname, x.attnum, bt.typname, (bt.typnamespace = 'pg_catalog'::regnamespace) as builtin,
		bt.typcategory::text as cat, bt.typtype::text as ttype,
		case when bt.typname = 'numeric' and x.emod >= 4 then ((x.emod - 4) >> 16) & 65535 end as nprec,
		case when bt.typname = 'numeric' and x.emod >= 4 then ((((x.emod - 4) & 2047) # 1024) - 1024) end as nscale,
		information_schema._pg_char_max_length(x.eoid, x.emod) as clen,
		information_schema._pg_datetime_precision(x.eoid, x.emod) as dtprec,
		x.attnotnull, x.dnotnull, x.nn_notvalid, x.identity, x.generated, x.defexpr, x.ddefault, x.coll_det
	from cols x join pg_type bt on bt.oid = x.eoid
	cross join ]] .. PIN
local cr = remote(col_q, 't."reloid", t."attname", t."attnum", t."typname", t."builtin", t."cat", t."ttype", t."nprec", t."nscale", t."clen", t."dtprec", t."attnotnull", t."dnotnull", t."nn_notvalid", t."identity", t."generated", t."defexpr", t."ddefault", t."coll_det", upper(t."attname")')
for i = 1, #cr do
	local x = cr[i]
	local r = REL_BY_OID[int(x[1])]
	if r then
		r.cols[#r.cols + 1] = {name = x[2], num = int(x[3]), typ = x[4], builtin = x[5], cat = x[6], ttype = x[7], nprec = int(x[8]),
		                       nscale = int(x[9]), clen = int(x[10]), dtprec = int(x[11]), notnull = x[12], dnotnull = x[13],
		                       nn_notvalid = x[14], identity = str(x[15]), generated = str(x[16]), defexpr = str(x[17]),
		                       ddefault = str(x[18]), coll_det = x[19], name_up = x[20]}
	end
end

-------------------------------------------------------------------------------------------------------------
-- type mapping: Exasol type + PostgreSQL source expression per column
-------------------------------------------------------------------------------------------------------------
local TS_LO, TZ_LO = "timestamp '0001-01-01 00:00:00'", "timestamp '0001-01-02 00:00:00'"
local function ts_hi(day, p)
	-- upper bound with exactly the column precision, so that a clamped value is stored unchanged
	return "timestamp '" .. day .. ' 23:59:59' .. (p > 0 and ('.' .. string.rep('9', p)) or '') .. "'"
end
local function oor(expr, lo, hi, typ)
	-- expr: PostgreSQL expression of a date/timestamp value (NULL passes); applies TEMPORAL_OUT_OF_RANGE
	if OORMODE == 'NULL' then
		return 'case when ' .. expr .. ' >= ' .. lo .. ' and ' .. expr .. ' <= ' .. hi .. ' then ' .. expr .. ' end'
	elseif OORMODE == 'CLAMP' then
		return 'case when ' .. expr .. ' < ' .. lo .. ' then ' .. lo .. ' when ' .. expr .. ' > ' .. hi .. ' then ' .. hi .. ' else ' .. expr .. ' end'
	end
	return 'case when ' .. expr .. ' is null or (' .. expr .. ' >= ' .. lo .. ' and ' .. expr .. ' <= ' .. hi .. ') then ' .. expr ..
	       ' else cast(' .. "'TEMPORAL_OUT_OF_RANGE: '" .. ' || ' .. expr .. '::text as ' .. typ .. ') end'
end
local function nan_guard(c, numeric)
	if numeric then
		return 'case when ' .. c .. "::text in ('NaN', 'Infinity', '-Infinity') then null else " .. c .. ' end'
	end
	local t = string.char(58, 58) .. 'float8'
	return "case when " .. c .. " in ('NaN'" .. t .. ", 'Infinity'" .. t .. ", '-Infinity'" .. t .. ") then null else " .. c .. " end"
end
local function textimp(c) if TRUNC then return 'left(' .. c .. '::text, 2000000)' end return c .. '::text' end

local function map_col(col)
	-- returns exa type, source expression, class ('dec','dbl','date','ts','tsz','bool','char','text','iv','other'), flags
	local c = qi(col.name)
	local t, cat, b = col.typ, col.cat, col.builtin
	local m = {cls = 'text'}
	if cat == 'P' or t == nil then m.unsupported = true return m end
	if b and (t == 'int2' or t == 'int4' or t == 'int8' or t == 'oid') then
		m.typ = ({int2 = 'DECIMAL(5,0)', int4 = 'DECIMAL(10,0)', int8 = 'DECIMAL(19,0)', oid = 'DECIMAL(10,0)'})[t]
		m.src = c; m.cls = 'dec'; m.p = ({int2 = 5, int4 = 10, int8 = 19, oid = 10})[t]; m.s = 0; m.exact = true
		if t == 'oid' then m.chk = c .. '::bigint' end
	elseif b and t == 'numeric' then
		local p, s = col.nprec, col.nscale
		if p ~= nil then
			if s < 0 then p, s = p - s, 0 elseif s > p then p = s end
		end
		if p == nil or p > 36 then
			m.overflow = true
			if DECOF == 'DOUBLE' then
				m.typ = 'DOUBLE'; m.cls = 'dbl'
				m.src = "case when " .. c .. "::text in ('NaN', 'Infinity', '-Infinity') then null when abs(" .. c .. ") < 2.4703282292062328e-324 then 0::float8 else " .. c .. "::float8 end"
			elseif DECOF == 'VARCHAR' then
				m.typ = 'VARCHAR(2000000) ASCII'; m.cls = 'numtext'
				if p == nil then
					m.src = "case when strpos(" .. c .. "::text, '.') > 0 then rtrim(rtrim(" .. c .. "::text, '0'), '.') else " .. c .. "::text end"
				else
					m.src = c .. '::text'
				end
			else
				-- numeric(p > 36, s) keeps its integer digits (up to 36) and rounds surplus fractional digits; unconstrained -> (36,18)
				local cs = (p == nil) and 18 or math.max(0, math.min(s, 36 - (p - s)))
				m.typ = 'DECIMAL(36,' .. istr(cs) .. ')'; m.cls = 'dec'; m.p = 36; m.s = cs; m.capped = true; m.nan_null = true
				m.src = "case when " .. c .. "::text in ('NaN', 'Infinity', '-Infinity') then null else round(" .. c .. ", " .. istr(cs) .. ") end"
			end
		else
			m.typ = 'DECIMAL(' .. istr(p) .. ',' .. istr(s) .. ')'; m.cls = 'dec'; m.p = p; m.s = s; m.exact = true; m.nan_null = true
			m.src = nan_guard(c, true)
		end
	elseif b and (t == 'float4' or t == 'float8') then
		m.typ = 'DOUBLE'; m.cls = 'dbl'; m.src = nan_guard(c, false)
	elseif b and t == 'money' then
		m.typ = 'DECIMAL(20,2)'; m.cls = 'dec'; m.p = 20; m.s = 2; m.exact = true; m.src = c .. '::numeric'; m.chk = c .. '::numeric'
	elseif b and t == 'bool' then
		m.typ = 'BOOLEAN'; m.cls = 'bool'; m.src = c
	elseif b and t == 'bpchar' then
		if col.clen ~= nil and col.clen <= 2000 then
			m.typ = 'CHAR(' .. istr(col.clen) .. ') UTF8'; m.cls = 'char'; m.src = c
		else
			m.typ = 'VARCHAR(' .. istr(math.min(col.clen or 2000000, 2000000)) .. ') UTF8'; m.src = textimp(c)
		end
	elseif b and t == 'char' then
		m.typ = 'VARCHAR(4) ASCII'; m.src = c .. '::text'
	elseif b and t == 'varchar' then
		if col.clen ~= nil and col.clen <= 2000000 then
			m.typ = 'VARCHAR(' .. istr(col.clen) .. ') UTF8'; m.src = c; m.plain_text = true
		else
			m.typ = 'VARCHAR(2000000) UTF8'; m.src = textimp(c); m.plain_text = true
		end
	elseif b and t == 'name' then
		m.typ = 'VARCHAR(128) UTF8'; m.src = c; m.plain_text = true
	elseif b and t == 'text' then
		m.typ = 'VARCHAR(2000000) UTF8'; m.src = TRUNC and ('left(' .. c .. ', 2000000)') or c; m.plain_text = true
	elseif b and t == 'date' then
		m.typ = 'DATE'; m.cls = 'date'; m.src = oor(c, "date '0001-01-01'", "date '9999-12-31'", 'date')
	elseif b and t == 'timestamp' then
		local p = (col.dtprec ~= nil and col.dtprec >= 0 and col.dtprec <= 9) and col.dtprec or 6
		m.typ = 'TIMESTAMP(' .. istr(p) .. ')'; m.cls = 'ts'; m.src = oor(c, TS_LO, ts_hi('9999-12-31', p), 'timestamp')
		m.oor = {c, TS_LO, '9999-12-31'}
	elseif b and t == 'timestamptz' then
		local p = (col.dtprec ~= nil and col.dtprec >= 0 and col.dtprec <= 9) and col.dtprec or 6
		m.typ = 'TIMESTAMP(' .. istr(p) .. ') WITH LOCAL TIME ZONE'; m.cls = 'tsz'; m.tstz = true
		m.src = oor('(' .. c .. " at time zone 'UTC')", TZ_LO, ts_hi('9999-12-30', p), 'timestamp')
		m.oor = {'(' .. c .. " at time zone 'UTC')", TZ_LO, '9999-12-30'}
	elseif b and t == 'time' then
		m.typ = 'VARCHAR(15) ASCII'; m.src = c .. '::text'
	elseif b and t == 'timetz' then
		m.typ = 'VARCHAR(24) ASCII'; m.src = c .. '::text'
	elseif b and t == 'interval' then
		m.cls = 'iv'
		if IVMODE == 'INTERVAL' then
			m.typ = 'INTERVAL DAY(9) TO SECOND(3)'
			local a = '(case when ' .. c .. " < interval '0' then -" .. c .. ' else ' .. c .. ' end)'
			local txt = '(case when ' .. c .. " < interval '0' then '-' else '' end) || extract(day from justify_hours(" .. a .. "))::bigint::text || ' ' || to_char(justify_hours(" .. a .. "), 'HH24:MI:SS.US')"
			local inf
			if OORMODE == 'NULL' then inf = 'null'
			elseif OORMODE == 'CLAMP' then inf = "case when " .. c .. " > interval '0' then '999999999 23:59:59.999' else '-999999999 23:59:59.999' end"
			else inf = "cast('TEMPORAL_OUT_OF_RANGE: ' || " .. c .. "::text as interval)::text" end
			-- beyond 999,999,999 days (Exasol DAY(9)) the value is out of range like infinity
			m.src = 'case when ' .. c .. ' is null then null when not isfinite(' .. c .. ') then ' .. inf ..
			        ' when extract(year from ' .. c .. ') = 0 and extract(month from ' .. c .. ') = 0 and extract(day from justify_hours(' .. a .. ')) > 999999999 then ' .. inf ..
			        ' when extract(year from ' .. c .. ') = 0 and extract(month from ' .. c .. ') = 0 then ' .. txt .. ' else ' .. c .. '::text end'
		else
			m.typ = 'VARCHAR(100) ASCII'; m.src = c .. '::text'
		end
	elseif b and t == 'bytea' then
		m.typ = 'VARCHAR(2000000) ASCII'; m.cls = 'bytea'
		if BINMODE == 'SKIP' then m.src = 'cast(null as varchar(10))'; m.skipped = true else m.src = 'translate(encode(' .. c .. ", 'base64'), chr(10), '')" end
	elseif b and t == 'uuid' then
		m.typ = 'CHAR(36) ASCII'; m.cls = 'uuid'; m.src = c .. '::text'
	elseif b and t == 'inet' then
		m.typ = 'VARCHAR(45) ASCII'; m.src = 'abbrev(' .. c .. ')'
	elseif b and t == 'cidr' then
		m.typ = 'VARCHAR(45) ASCII'; m.src = c .. '::text'
	elseif b and t == 'macaddr' then
		m.typ = 'VARCHAR(17) ASCII'; m.src = c .. '::text'
	elseif b and t == 'macaddr8' then
		m.typ = 'VARCHAR(23) ASCII'; m.src = c .. '::text'
	elseif b and cat == 'V' then
		m.typ = 'VARCHAR(' .. istr(math.min(col.clen or 2000000, 2000000)) .. ') ASCII'; m.src = textimp(c)
	elseif cat == 'E' then
		m.typ = 'VARCHAR(63) UTF8'; m.src = c .. '::text'; m.plain_text = true
	else
		m.typ = 'VARCHAR(2000000) UTF8'; m.src = textimp(c)
		if t == 'json' or t == 'jsonb' then m.json = true end
		if col.ttype == 'c' or cat == 'C' then m.composite = true end
	end
	return m
end

-- default mapping by target type; returns ' DEFAULT ...' or '' (+ reason when skipped)
local function map_default(col, m)
	if col.generated ~= nil and col.generated ~= '' then return '' end
	if col.identity ~= nil and col.identity ~= '' then return '' end
	local d = col.defexpr or col.ddefault
	if d == nil or d == '' then return '' end
	if d:find('^nextval%(') then return '' end
	if d:upper() == 'NULL' or d:upper():sub(1, 6) == 'NULL' .. string.char(58, 58) then return '' end   -- DEFAULT NULL = no default
	local u = d:upper()
	local cls = m.cls
	local function curfun()
		local f = u:gsub('%(%d+%)$', '')
		return f == 'NOW()' or f == 'CURRENT_TIMESTAMP' or f == 'TRANSACTION_TIMESTAMP()' or f == 'STATEMENT_TIMESTAMP()' or f == 'CLOCK_TIMESTAMP()' or f == 'LOCALTIMESTAMP'
	end
	if cls == 'ts' or cls == 'tsz' then
		if curfun() then return ' DEFAULT CURRENT_TIMESTAMP' end
		if cls == 'ts' then
			local v = d:match("^'(%d%d%d%d%-%d%d%-%d%d %d%d:%d%d:%d%d[%.%d]*)'::timestamp")
			if v then return ' DEFAULT TIMESTAMP ' .. sl(v) end
			v = d:match("^'(%d%d%d%d%-%d%d%-%d%d)'::timestamp")
			if v then return ' DEFAULT TIMESTAMP ' .. sl(v .. ' 00:00:00') end
		end
		return nil, d
	elseif cls == 'date' then
		if u == 'CURRENT_DATE' then return ' DEFAULT CURRENT_DATE' end
		local v = d:match("^'(%d%d%d%d%-%d%d%-%d%d)'::date$")
		if v then return ' DEFAULT DATE ' .. sl(v) end
		return nil, d
	elseif cls == 'bool' then
		if u == 'TRUE' or u == 'FALSE' then return ' DEFAULT ' .. u end
		return nil, d
	elseif cls == 'dec' or cls == 'dbl' then
		local v = d:match("^'([%-%d%.]+)'::%a") or d:match('^([%-%d%.]+)$') or d:match('^%(([%-%d%.]+)%)$')
		local a = v and v:gsub('^%-', '') or nil
		if a and (a:match('^%d+$') or a:match('^%d+%.%d+$')) then
			if cls == 'dec' and m.p then
				local ip = v:gsub('^%-', ''):gsub('%..*$', ''):gsub('^0+', '')
				if #ip > m.p - m.s then return nil, d end
			end
			local digits = v:gsub('[%-%.]', ''):gsub('^0+', '')
			if #digits > 36 or #v > 2000 then return nil, d end       -- beyond DECIMAL(36) / the 2,000-character default limit
			return ' DEFAULT ' .. v
		end
		return nil, d
	elseif (m.typ:sub(1, 7) == 'VARCHAR' or m.typ:sub(1, 4) == 'CHAR') and cls ~= 'bytea' and cls ~= 'iv' and cls ~= 'numtext' then
		if col.builtin and (col.typ == 'time' or col.typ == 'timetz') then
			local v = d:match("^'([%d:%.%+%-]+)'::time")
			if v then return ' DEFAULT ' .. sl(v) end
			return nil, d
		end
		local v = d:match("^'(.*)'::[%w_%s%.%(%)" .. DQ .. "%[%]]+$") or d:match("^'(.*)'$")
		if v ~= nil and not (v:gsub(SQ .. SQ, '')):find(SQ) then
			v = v:gsub(SQ .. SQ, SQ)
			if v == '' then return nil, d end
			local maxlen = tonumber(m.typ:match('%((%d+)%)'))
			if maxlen and ulen(v) > maxlen then return nil, d end
			if ulen(sl(v)) > 2000 then return nil, d end            -- Exasol: a default value holds at most 2,000 characters
			return ' DEFAULT ' .. sl(v)
		end
		return nil, d
	end
	return nil, d
end

-- per relation: map columns, renames, collisions, notes
for _, r in ipairs(RELS) do
	if r.migrate then
		table.sort(r.cols, function(a, b) return a.num < b.num end)
		local used, collide = {}, false
		local reserved, allnames = {}, {}
		for _, col in ipairs(r.cols) do allnames[exa_id(col.name, col.name_up)] = true end
		for _, col in ipairs(r.cols) do
			col.m = map_col(col)
			if col.m.unsupported then
				note('UNSUPPORTED TYPE', 'column ' .. r.label .. '.' .. oneline(col.name) .. ' (PostgreSQL type ' .. tostring(col.typ) .. ') is not migrated', true)
			else
				local en = exa_id(col.name, col.name_up)
				if FORBIDDEN_COL[en] then
					local nn = en .. '_'
					while used[nn] or allnames[nn] do nn = nn .. '_' end
					note('RENAMED COLUMN', 'column ' .. r.label .. '.' .. oneline(col.name) .. ' is migrated as ' .. oneline(nn) .. ' (' .. en .. ' is not allowed as an Exasol column name)', true)
					en = nn
				end
				if used[en] then
					collide = true
					note('NAME COLLISION', 'columns of ' .. r.label .. ' map to the same Exasol column name ' .. oneline(en) .. ' - the table is not migrated; use IDENTIFIER_CASE_INSENSITIVE = false', true)
				end
				used[en] = true
				col.exa = en
				if RESERVED[en:upper()] then reserved[#reserved + 1] = oneline(en) end
				if col.m.tstz then r.has_tstz = true end
				if not col.coll_det or (not col.builtin and col.typ == 'citext') then
					note('CASE-INSENSITIVE KEY', 'column ' .. r.label .. '.' .. oneline(col.name) .. ' compares case-insensitively in PostgreSQL (citext or nondeterministic collation); Exasol compares binary - joins and keys on it may behave differently', true)
				end
				if col.m.overflow and DECOF == 'VARCHAR' then r.numtext = true end
			end
		end
		if #r.cols == 0 then
			r.migrate = false
			note('NO COLUMNS', 'table ' .. r.label .. ' has no columns and is not migrated', true)
		end
		if collide then r.migrate = false end
		if #reserved > 0 then note('RESERVED NAME', 'table ' .. r.label .. ' has columns whose names are reserved words in Exasol and must be quoted in queries: ' .. table.concat(reserved, ', '), false) end
		if r.migrate and RESERVED[r.exa_table:upper()] then note('RESERVED NAME', 'the table name ' .. oneline(r.exa_table) .. ' (' .. r.label .. ') is a reserved word in Exasol and must be quoted in queries', false) end
		if TRUNC then
			for _, col in ipairs(r.cols) do
				if not col.m.unsupported and (col.m.json or col.m.composite or col.cat == 'A' or col.cat == 'R') then r.trunc_struct = true end
			end
		end
		r.colmap = {}
		for _, col in ipairs(r.cols) do r.colmap[col.name] = col end
	end
end
do
	local any = false
	for _, r in ipairs(RELS) do if r.migrate and r.trunc_struct then any = true end end
	if any then note('TRUNCATE', 'TRUNCATE_LONG_STRINGS = true cuts long json, array, range or composite values in the middle - such a value is no longer valid; the data validation counts invalid json', true) end
end

-------------------------------------------------------------------------------------------------------------
-- primary keys and foreign keys
-------------------------------------------------------------------------------------------------------------
local pk_q = [[select c.oid::bigint, con.conname, att.attname, k.ord::int, coalesce((to_jsonb(con) ->> 'conperiod')::boolean, false)
	from pg_constraint con join pg_class c on c.oid = con.conrelid join pg_namespace n on n.oid = c.relnamespace
	join unnest(con.conkey) with ordinality k(attnum, ord) on true
	join pg_attribute att on att.attrelid = con.conrelid and att.attnum = k.attnum
	where con.contype = 'p' and not c.relispartition and]] .. RELFILTER
local pr = remote(pk_q)
local PK = {}
for i = 1, #pr do
	local r = REL_BY_OID[int(pr[i][1])]
	if r and r.migrate then
		PK[r.oid] = PK[r.oid] or {name = pr[i][2], cols = {}, temporal = pr[i][5]}
		PK[r.oid].cols[int(pr[i][4])] = pr[i][3]
	end
end

-- foreign keys: the parent may be in another schema (outside the filter) - parent filter is applied in Lua
local fk_q = [[select c.oid::bigint as coid, con.conname, ca.attname as ccol, k.ord::int,
	fc.oid::bigint as poid, fn.nspname, fc.relname, fa.attname as pcol,
	array_position(pk.conkey, fk.attnum) as pkpos, coalesce(array_length(pk.conkey, 1), 0) as pklen, array_length(con.conkey, 1) as fklen,
	con.convalidated, coalesce((to_jsonb(con) ->> 'conenforced')::boolean, true), coalesce((to_jsonb(con) ->> 'conperiod')::boolean, false)
	from pg_constraint con join pg_class c on c.oid = con.conrelid join pg_namespace n on n.oid = c.relnamespace
	join pg_class fc on fc.oid = con.confrelid join pg_namespace fn on fn.oid = fc.relnamespace
	join unnest(con.conkey) with ordinality k(attnum, ord) on true
	join pg_attribute ca on ca.attrelid = con.conrelid and ca.attnum = k.attnum
	join unnest(con.confkey) with ordinality fk(attnum, ord) on fk.ord = k.ord
	join pg_attribute fa on fa.attrelid = con.confrelid and fa.attnum = fk.attnum
	left join pg_constraint pk on pk.conrelid = con.confrelid and pk.contype = 'p'
	where con.contype = 'f' and con.conparentid = 0 and not c.relispartition and]] .. RELFILTER
local fr = remote(fk_q)
local FKS, FK_ORDER = {}, {}
for i = 1, #fr do
	local x = fr[i]
	local key = istr(int(x[1])) .. NL .. x[2]
	local f = FKS[key]
	if not f then
		f = {coid = int(x[1]), name = x[2], poid = int(x[5]), pnsp = x[6], prel = x[7], pairs = {}, pklen = int(x[10]), fklen = int(x[11]),
		     valid = x[12], enforced = x[13], temporal = x[14]}
		FKS[key] = f; FK_ORDER[#FK_ORDER + 1] = key
	end
	f.pairs[#f.pairs + 1] = {ccol = x[3], pcol = x[8], pkpos = int(x[9])}
end
table.sort(FK_ORDER)

local function widen(ct, pt)
	-- returns the type the child column gets so that its type equals the parent key type, or nil
	if ct == pt then return ct end
	local cp, cs = ct:match('^DECIMAL%((%d+),(%d+)%)$')
	local pp, ps = pt:match('^DECIMAL%((%d+),(%d+)%)$')
	if cp and pp then
		cp, cs, pp, ps = tonumber(cp), tonumber(cs), tonumber(pp), tonumber(ps)
		if pp - ps >= cp - cs and ps >= cs then return pt end
		return nil
	end
	local tp, tz = ct:match('^TIMESTAMP%((%d+)%)(.*)$')
	local pp2, pz = pt:match('^TIMESTAMP%((%d+)%)(.*)$')
	if tp and pp2 and tz == pz and tonumber(pp2) >= tonumber(tp) then return pt end
	local cl, cc = ct:match('^VARCHAR%((%d+)%) (%w+)$')
	local pl, pc = pt:match('^VARCHAR%((%d+)%) (%w+)$')
	if cl and pl and cc == pc and tonumber(pl) >= tonumber(cl) then return pt end
	return nil
end

-- why a foreign key cannot be migrated, independent of the column types (nil = candidate)
local function fk_reason(f, cr_, pr_)
	if not pr_ or not pr_.migrate then return 'its parent table ' .. oneline(f.pnsp) .. '.' .. oneline(f.prel) .. ' is not migrated' end
	if f.temporal then return 'it is a temporal (PERIOD) foreign key' end
	if f.pklen == 0 or f.pklen ~= f.fklen then return 'it does not reference the primary key of its parent (Exasol foreign keys always reference the primary key)' end
	for _, p in ipairs(f.pairs) do
		if p.pkpos == nil then return 'it does not reference the primary key of its parent (Exasol foreign keys always reference the primary key)' end
	end
	for _, p in ipairs(f.pairs) do
		local cc, pc = cr_.colmap[p.ccol], pr_.colmap[p.pcol]
		if not cc or not pc or cc.m.unsupported or pc.m.unsupported then return 'a key column is not migrated' end
		if cc.m.skipped or pc.m.skipped then return 'a key column is not transferred (BINARY_HANDLING = SKIP)' end
	end
	return nil
end
-- widening to a fixpoint: a key column gets the type of the key it references, also across chains (grandchild -> child -> parent)
do
	local changed, rounds = true, 0
	while changed and rounds < #FK_ORDER + 2 do
		changed, rounds = false, rounds + 1
		for _, key in ipairs(FK_ORDER) do
			local f = FKS[key]
			local cr_, pr_ = REL_BY_OID[f.coid], REL_BY_OID[f.poid]
			if cr_ and cr_.migrate and not fk_reason(f, cr_, pr_) then
				-- widen only when every column pair of the key can be widened (otherwise the key is skipped anyway)
				local todo, ok = {}, true
				for _, p in ipairs(f.pairs) do
					local cc, pc = cr_.colmap[p.ccol], pr_.colmap[p.pcol]
					local cur = cc.newtyp or cc.m.typ
					local nt = widen(cur, pc.newtyp or pc.m.typ)
					if not nt then ok = false break end
					if nt ~= cur then todo[#todo + 1] = {cc, nt} end
				end
				if ok then for _, t in ipairs(todo) do t[1].newtyp = t[2]; changed = true end end
			end
		end
	end
end
for _, r in ipairs(RELS) do
	if r.migrate then
		for _, col in ipairs(r.cols) do
			if col.newtyp and col.m.oor then
				-- a widened timestamp precision needs the clamp bound of the new precision
				local q = tonumber(col.newtyp:match('^TIMESTAMP%((%d+)%)'))
				if q then col.m.src = oor(col.m.oor[1], col.m.oor[2], ts_hi(col.m.oor[3], q), 'timestamp') end
			end
			if col.newtyp then
				note('WIDENED COLUMN', 'column ' .. r.label .. '.' .. oneline(col.name) .. ' is created as ' .. col.newtyp .. ' (the type of the referenced primary key) instead of ' .. col.m.typ, false)
			end
		end
	end
end
local FK_OUT = {}
local FK_SIG = {}                  -- Exasol accepts one foreign key per child column list and parent
for _, key in ipairs(FK_ORDER) do
	local f = FKS[key]
	local cr_ = REL_BY_OID[f.coid]
	local pr_ = REL_BY_OID[f.poid]
	local flabel = cr_ and (cr_.label .. ' (' .. oneline(f.name) .. ')') or oneline(f.name)
	if cr_ and cr_.migrate then
		local why = fk_reason(f, cr_, pr_)
		if not why then
			table.sort(f.pairs, function(a, b) return a.pkpos < b.pkpos end)
			for _, p in ipairs(f.pairs) do
				local cc, pc = cr_.colmap[p.ccol], pr_.colmap[p.pcol]
				local ct, pt = cc.newtyp or cc.m.typ, pc.newtyp or pc.m.typ
				if ct ~= pt then why = 'the column types differ (' .. ct .. ' / ' .. pt .. ') and cannot be widened losslessly' break end
			end
			if not why then
				local sig = {istr(f.coid), istr(f.poid)}
				for _, p in ipairs(f.pairs) do sig[#sig + 1] = p.ccol end
				sig = table.concat(sig, NL)
				local kept = FK_SIG[sig]
				if kept then
					-- the kept key may be enabled when either duplicate is valid and enforced in PostgreSQL
					kept.valid = kept.valid or f.valid
					kept.enforced = kept.enforced or f.enforced
					why = 'it is identical to foreign key ' .. oneline(kept.name) .. ' (same columns and parent; Exasol allows only one)'
				else
					FK_SIG[sig] = f
					f.exa_name = f.name
					FK_OUT[#FK_OUT + 1] = f
				end
			end
		end
		if why then note('SKIPPED FOREIGN KEY', 'foreign key ' .. flabel .. ' is not migrated because ' .. why, true) end
	end
end
-- FK names: folded with Exasol UPPER (like all identifiers); names that collide on one table get a suffix
if ICI and #FK_OUT > 0 then
	local parts = {}
	for i, f in ipairs(FK_OUT) do parts[#parts + 1] = 'select ' .. istr(i) .. ' as i, upper(' .. sl(f.name) .. ') as u from sys.dual' end
	for k = 1, #parts, 500 do
		local ur = query('select i, u from (' .. table.concat(parts, ' union all ', k, math.min(k + 499, #parts)) .. ')')
		for j = 1, #ur do FK_OUT[int(ur[j][1])].exa_name = ur[j][2] end
	end
end
-- output order: by child schema, child table, constraint name (not by catalog oid)
table.sort(FK_OUT, function(a, b)
	local ca, cb = REL_BY_OID[a.coid], REL_BY_OID[b.coid]
	if ca.exa_schema ~= cb.exa_schema then return ca.exa_schema < cb.exa_schema end
	if ca.exa_table ~= cb.exa_table then return ca.exa_table < cb.exa_table end
	if a.exa_name ~= b.exa_name then return a.exa_name < b.exa_name end
	return a.name < b.name
end)
do
	local seen = {}
	-- the synthesized primary key names (<table>_PK) are taken on their tables
	for oid in pairs(PK) do
		local r = REL_BY_OID[oid]
		if r then seen[istr(oid) .. NL .. r.exa_table .. '_PK'] = true end
	end
	for _, f in ipairs(FK_OUT) do
		local base, n = f.exa_name, 1
		while seen[istr(f.coid) .. NL .. f.exa_name] do n = n + 1; f.exa_name = base .. '_' .. istr(n) end
		seen[istr(f.coid) .. NL .. f.exa_name] = true
	end
end

-- key columns transported as text must compare like in PostgreSQL: canonical text form
do
	local keycols = {}
	local function mark(r, name) if r and name and r.colmap[name] then keycols[#keycols + 1] = {r, r.colmap[name]} end end
	for oid, pk in pairs(PK) do for _, n in pairs(pk.cols) do mark(REL_BY_OID[oid], n) end end
	for _, f in ipairs(FK_OUT) do
		for _, p in ipairs(f.pairs) do mark(REL_BY_OID[f.coid], p.ccol); mark(REL_BY_OID[f.poid], p.pcol) end
	end
	local INET_MIX = {}
	for _, f in ipairs(FK_OUT) do
		for _, p in ipairs(f.pairs) do
			local cc, pc = REL_BY_OID[f.coid].colmap[p.ccol], REL_BY_OID[f.poid].colmap[p.pcol]
			if cc.builtin and pc.builtin and cc.typ ~= pc.typ and (cc.typ == 'inet' or cc.typ == 'cidr') and (pc.typ == 'inet' or pc.typ == 'cidr') then
				INET_MIX[cc] = true; INET_MIX[pc] = true
			end
		end
	end
	for _, rc in ipairs(keycols) do
		local r, col = rc[1], rc[2]
		local m = col.m
		if not col.canon and not m.unsupported then
			local c = qi(col.name)
			if m.cls == 'iv' and IVMODE == 'VARCHAR' then
				m.src = 'justify_interval(' .. c .. ')::text'; col.canon = true
				note('KEY TEXT', 'interval key column ' .. r.label .. '.' .. oneline(col.name) .. ' is transferred in canonical form (justify_interval) so that equal intervals stay equal in Exasol', false)
			elseif INET_MIX[col] then
				m.src = 'abbrev(' .. c .. '::inet)'; col.canon = true
				note('KEY TEXT', 'key column ' .. r.label .. '.' .. oneline(col.name) .. ' (' .. col.typ .. ', foreign key between inet and cidr) is transferred in inet output form (abbrev of inet) so that both sides stay equal in Exasol', false)
			elseif m.cls == 'numtext' then
				m.src = "case when strpos(" .. c .. "::text, '.') > 0 then rtrim(rtrim(" .. c .. "::text, '0'), '.') else " .. c .. "::text end"; col.canon = true
				note('KEY TEXT', 'numeric key column ' .. r.label .. '.' .. oneline(col.name) .. ' is transferred as text without trailing zeros so that equal numbers stay equal in Exasol', false)
			end
		end
	end
end

-------------------------------------------------------------------------------------------------------------
-- partitions (single-column keys on a partitionable mapped type)
-------------------------------------------------------------------------------------------------------------
local PARTS = {}
if G_PART then
	local part_q = [=[select c.oid::bigint, regexp_replace(pg_get_partkeydef(c.oid), '[[:cntrl:]]', ' ', 'g'), pt.partnatts::int,
		(select a.attname from pg_attribute a where a.attrelid = c.oid and a.attnum = pt.partattrs[0] and pt.partattrs[0] <> 0)
		from pg_partitioned_table pt join pg_class c on c.oid = pt.partrelid join pg_namespace n on n.oid = c.relnamespace
		where not c.relispartition and]=] .. RELFILTER .. [[ order by n.nspname, c.relname]]
	local qr = remote(part_q)
	for i = 1, #qr do
		local r = REL_BY_OID[int(qr[i][1])]
		if r and r.migrate then
			local col = (int(qr[i][3]) == 1 and not isnull(qr[i][4])) and r.colmap[qr[i][4]] or nil
			local t = col and not col.m.unsupported and (col.newtyp or col.m.typ) or ''
			local ok = t:match('^DECIMAL') or t:match('^DOUBLE') or t:match('^DATE') or t:match('^TIMESTAMP') or t:match('^BOOLEAN') or t:match('^INTERVAL')
			if ok then
				PARTS[#PARTS + 1] = 'ALTER TABLE ' .. qi(r.exa_schema) .. '.' .. qi(r.exa_table) .. ' PARTITION BY ' .. qi(col.exa) .. ';'
			else
				note('PARTITION', 'table ' .. oneline(r.exa_schema) .. '.' .. oneline(r.exa_table) .. ' - PostgreSQL partitioning ' .. qr[i][2] ..
				     ' is not mapped automatically (Exasol partitions on one column of a numeric, date, timestamp, boolean or interval type); add PARTITION BY manually if appropriate', false)
			end
		end
	end
	table.sort(PARTS)
end

-------------------------------------------------------------------------------------------------------------
-- comments
-------------------------------------------------------------------------------------------------------------
local COMMENTS = {}
local VIEW_COMMENTS = {}
if G_COMM or G_VIEWS then
	local cm_q = [[select 'S' as kind, 0::bigint, n.nspname, null::text, left(d.description, 2000)
		from pg_description d join pg_namespace n on n.oid = d.objoid
		where d.classoid = 'pg_namespace'::regclass and left(n.nspname, 3) <> 'pg_' and n.nspname <> 'information_schema' and n.nspname like ]] .. pg_sf .. [[
		union all
		select case when d.objsubid = 0 then 'T' else 'C' end, c.oid::bigint, n.nspname, a.attname, left(d.description, 2000)
		from pg_description d join pg_class c on c.oid = d.objoid join pg_namespace n on n.oid = c.relnamespace
		left join pg_attribute a on a.attrelid = c.oid and a.attnum = d.objsubid
		where d.classoid = 'pg_class'::regclass and c.relkind in ('r', 'p', 'm', 'v') and not c.relispartition and]] .. RELFILTER
	local qr = remote(cm_q)
	local schema_done = {}
	for i = 1, #qr do
		local kind, oid, nsp, att, txt = qr[i][1], int(qr[i][2]), qr[i][3], qr[i][4], qr[i][5]
		if kind == 'S' then
			-- only for schemas that receive migrated tables (and only when TARGET_SCHEMA is not set)
			for _, r in ipairs(RELS) do
				if r.migrate and r.nsp == nsp and not TGT and not schema_done[r.exa_schema] then
					schema_done[r.exa_schema] = true
					if G_COMM then COMMENTS[#COMMENTS + 1] = {r.exa_schema, '', 0, 'COMMENT ON SCHEMA ' .. qi(r.exa_schema) .. ' IS ' .. sl(txt) .. ';'} end
				end
			end
		else
			local r = REL_BY_OID[oid]
			if r and r.migrate and G_COMM then
				if kind == 'T' then
					COMMENTS[#COMMENTS + 1] = {r.exa_schema, r.exa_table, 0, 'COMMENT ON TABLE ' .. qi(r.exa_schema) .. '.' .. qi(r.exa_table) .. ' IS ' .. sl(txt) .. ';'}
				else
					local col = r.colmap[att]
					if col and col.exa then
						COMMENTS[#COMMENTS + 1] = {r.exa_schema, r.exa_table, col.num, 'COMMENT ON COLUMN ' .. qi(r.exa_schema) .. '.' .. qi(r.exa_table) .. '.' .. qi(col.exa) .. ' IS ' .. sl(txt) .. ';'}
					end
				end
			elseif r and (r.kind == 'v' or r.kind == 'm') then
				VIEW_COMMENTS[oid] = VIEW_COMMENTS[oid] or {}
				VIEW_COMMENTS[oid][#VIEW_COMMENTS[oid] + 1] = (kind == 'T' and 'comment' or ('column ' .. oneline(att) .. ' comment')) .. ' - ' .. txt
			end
		end
	end
	table.sort(COMMENTS, function(a, b) if a[1] ~= b[1] then return a[1] < b[1] end if a[2] ~= b[2] then return a[2] < b[2] end return a[3] < b[3] end)
end

-------------------------------------------------------------------------------------------------------------
-- parallel ranges
-------------------------------------------------------------------------------------------------------------
local LEAVES = {}
if PS > 1 then
	local lq = [[select c.oid::bigint, (pg_relation_size(l.relid) / current_setting('block_size')::int)::bigint, coalesce(lc.reltuples, -1)::float8, lc.relpages::bigint, lc.relkind::text
		from pg_class c join pg_namespace n on n.oid = c.relnamespace
		cross join lateral pg_partition_tree(c.oid) l join pg_class lc on lc.oid = l.relid
		where c.relkind = 'p' and not c.relispartition and l.isleaf and]] .. RELFILTER
	local qr = remote(lq)
	for i = 1, #qr do
		local oid = int(qr[i][1])
		LEAVES[oid] = LEAVES[oid] or {}
		LEAVES[oid][#LEAVES[oid] + 1] = {blocks = int(qr[i][2]), reltuples = tonumber(qr[i][3]), relpages = int(qr[i][4]), kind = qr[i][5]}
	end
end
local function est_rows(blocks, reltuples, relpages)
	-- nil = unknown (never analyzed, or analyzed while empty and filled since then)
	if reltuples == nil or reltuples < 0 then return nil end
	if relpages ~= nil and relpages > 0 then return reltuples / relpages * blocks end
	if blocks ~= nil and blocks > 0 then return nil end
	return reltuples
end
local function ranges(r, nmax)
	-- returns a list of {lo, hi} block bounds (nil = open), or nil for a single statement
	local PS = math.min(PS, nmax or PS)
	if PS <= 1 then return nil end
	local blocks, est, sizes = 0, 0, nil
	if r.kind == 'p' then
		if r.nonheap_part then return nil end
		local lv = LEAVES[r.oid] or {}
		sizes = {}
		local unknown = false
		for _, l in ipairs(lv) do
			if l.kind == 'f' then return nil end
			blocks = blocks + l.blocks; sizes[#sizes + 1] = l.blocks
			local e = est_rows(l.blocks, l.reltuples, l.relpages)
			if e == nil and l.blocks > 0 then unknown = true else est = est + (e or 0) end
		end
		if unknown then est = nil end
	else
		if r.foreign_part then return nil end
		if r.kind ~= 'p' and r.am ~= 'heap' then return nil end      -- ctid block ranges need the heap access method
		blocks = r.blocks
		est = est_rows(r.blocks, r.reltuples, r.relpages)
	end
	-- without usable statistics the row count is estimated from the size (about one row per 80 bytes)
	if est == nil then est = blocks * math.floor(BS / 80) end
	if est < PMIN then return nil end
	local n = math.min(PS, blocks)
	if n <= 1 then return nil end
	local bounds = {}
	if sizes then
		local total, maxb = 0, 0
		for _, s in ipairs(sizes) do total = total + s; if s > maxb then maxb = s end end
		for k = 1, n - 1 do
			local target = k * total / n
			local lo, hi = 0, maxb
			while lo < hi do
				local mid = math.floor((lo + hi) / 2)
				local sum = 0
				for _, s in ipairs(sizes) do sum = sum + math.min(mid, s) end
				if sum >= target then hi = mid else lo = mid + 1 end
			end
			bounds[#bounds + 1] = lo
		end
	else
		for k = 1, n - 1 do bounds[#bounds + 1] = math.floor(k * blocks / n) end
	end
	local uniq, last = {}, 0
	for _, b in ipairs(bounds) do if b > last then uniq[#uniq + 1] = b; last = b end end
	if #uniq == 0 then return nil end
	local out = {}
	out[1] = {nil, uniq[1]}
	for k = 2, #uniq do out[#out + 1] = {uniq[k - 1], uniq[k]} end
	out[#out + 1] = {uniq[#uniq], nil}
	return out
end

-------------------------------------------------------------------------------------------------------------
-- generate
-------------------------------------------------------------------------------------------------------------
local SIZE_LIMIT = 120000
local function from_clause(r)
	return ' from ' .. ((r.kind == 'r') and 'only ' or '') .. qi(r.nsp) .. '.' .. qi(r.rel) .. ' as "__src" cross join ' .. PIN
end

-- header
add('-- ### PostgreSQL -> Exasol migration generated by ' .. SCRIPT_SCHEMA .. '.' .. exa.meta.script_name .. ' ###')
add('-- source PostgreSQL ' .. oneline(VER) .. ', server_encoding ' .. oneline(ENC) .. '; generated in an Exasol session with TIME_ZONE ' .. SESSION_TZ)
add('-- PARALLEL_STATEMENTS = ' .. PS_NOTE .. '; PARALLEL_MIN_ROWS = ' .. istr(PMIN))
if PS > 1 then note('PARALLEL', 'tables with at least ' .. istr(PMIN) .. ' estimated rows are read with up to ' .. istr(PS) .. ' parallel STATEMENTs; each STATEMENT is a separate PostgreSQL transaction, so the source must not be written during the IMPORTs (otherwise rows can be duplicated or missed) - verify with CHECK_MIGRATION', true)
elseif VNUM < 140000 then note('VERSION', PS_NOTE, false) end

local migrated = {}
for _, r in ipairs(RELS) do if r.migrate then migrated[#migrated + 1] = r end end

-- everything that can add a note is computed before the note section is written
local ROW_LIMIT = 1990000          -- one output row is VARCHAR(2000000)
for _, r in ipairs(migrated) do
	-- column definitions
	r.defs = {}
	for _, col in ipairs(r.cols) do
		if not col.m.unsupported then
			local m = col.m
			local typ = col.newtyp or m.typ
			local d, why = map_default(col, m)
			if d == nil then
				d = ''
				note('SKIPPED DEFAULT', 'column ' .. r.label .. '.' .. oneline(col.name) .. ' default ' .. oneline(why) .. ' is not migrated (not representable for ' .. typ .. ')', false)
			end
			local nn = ''
			local nn_src = (col.notnull and not col.nn_notvalid) or col.dnotnull
			local nn_ok = m.cls == 'dec' or m.cls == 'date' or m.cls == 'ts' or m.cls == 'tsz' or m.cls == 'bool'
			if (m.cls == 'date' or m.cls == 'ts' or m.cls == 'tsz') and OORMODE == 'NULL' then nn_ok = false end
			if m.nan_null then nn_ok = false end            -- numeric NaN / +-Infinity are loaded as NULL
			if nn_src and nn_ok then nn = ' NOT NULL' end
			r.defs[#r.defs + 1] = qi(col.exa) .. ' ' .. typ .. d .. nn
		end
	end
	-- primary key
	local pk = PK[r.oid]
	if pk then
		local cols, ok = {}, true
		for i = 1, #pk.cols do
			local col = r.colmap[pk.cols[i]]
			if not col or not col.exa then ok = false break end
			if col.m.skipped then ok = false; note('SKIPPED PRIMARY KEY', 'primary key of ' .. r.label .. ' is not migrated because its column ' .. oneline(col.name) .. ' is not transferred (BINARY_HANDLING = SKIP)', true) break end
			cols[#cols + 1] = qi(col.exa)
		end
		if pk.temporal then ok = false; note('SKIPPED PRIMARY KEY', 'primary key of ' .. r.label .. ' is a temporal (WITHOUT OVERLAPS) key and is not migrated', true) end
		if ok then
			r.pk_name = r.exa_table .. '_PK'
			r.pk_line = 'ALTER TABLE ' .. qi(r.exa_schema) .. '.' .. qi(r.exa_table) .. ' ADD CONSTRAINT ' .. qi(r.pk_name) .. ' PRIMARY KEY (' .. table.concat(cols, ', ') .. ') DISABLE;'
		end
	end
	-- IMPORT
	if not (r.kind == 'm' and not r.populated) then
		local tcols, srcs = {}, {}
		for _, col in ipairs(r.cols) do
			if not col.m.unsupported then tcols[#tcols + 1] = qi(col.exa); srcs[#srcs + 1] = col.m.src end
		end
		local base = 'select ' .. table.concat(srcs, ', ') .. from_clause(r)
		local head = 'IMPORT INTO ' .. qi(r.exa_schema) .. '.' .. qi(r.exa_table) .. ' (' .. table.concat(tcols, ', ') .. ') FROM JDBC AT ' .. CONN
		local function build(nmax)
			local rg = ranges(r, nmax)
			local stmts, rawmax = {}, 0
			local raws = {}
			if rg then
				for _, b in ipairs(rg) do
					local w = {}
					if b[1] then w[#w + 1] = '"__src".ctid >= ' .. sl('(' .. istr(b[1]) .. ',0)') .. '::tid' end
					if b[2] then w[#w + 1] = '"__src".ctid < ' .. sl('(' .. istr(b[2]) .. ',0)') .. '::tid' end
					raws[#raws + 1] = base .. ' where ' .. table.concat(w, ' and ')
				end
			else
				raws[1] = base
			end
			local len, maxl = #head + 80, 0
			for _, raw in ipairs(raws) do
				local q = sl(raw)
				stmts[#stmts + 1] = q
				len = len + #q + 11
				if #q > maxl then maxl = #q end
				if #raw > rawmax then rawmax = #raw end          -- the engine limit applies to the statement text itself
			end
			return stmts, len, maxl, rawmax
		end
		local stmts, len, maxl, rawmax = build(nil)
		if #stmts > 1 and len > ROW_LIMIT then
			local nmax = math.floor((ROW_LIMIT - #head - 80) / (maxl + 11))
			stmts, len, maxl, rawmax = build(nmax)
			note('PARALLEL', 'table ' .. r.label .. ' is read with ' .. istr(#stmts) .. ' instead of up to ' .. istr(PS) .. ' parallel STATEMENTs: every STATEMENT repeats the full select list, and its IMPORT must fit into one output row of 2,000,000 characters', false)
		end
		if rawmax >= SIZE_LIMIT then
			note('SIZE LIMIT', 'the longest STATEMENT of the IMPORT of ' .. r.label .. ' is ' .. istr(rawmax) .. ' bytes long; Exasol rejects a STATEMENT of 131072 bytes or more (ETL-1100) - reduce the number of columns, or use TEMPORAL_OUT_OF_RANGE = NULL (shortest expressions; out-of-range values then load as NULL)', true)
		end
		local parts = {}
		for _, s in ipairs(stmts) do parts[#parts + 1] = ' STATEMENT ' .. s end
		r.import_row = head .. table.concat(parts) .. ';' .. (r.has_tstz and ('  -- requires session TIME_ZONE = ' .. SQ .. 'UTC' .. SQ .. ' (see above)') or '')
	end
end

-- notes first (they explain what follows)
for _, n in ipairs(NOTES) do add(n) end

if #migrated > 0 then
	add('-- ### SCHEMAS ###')
	local sdone = {}
	for _, r in ipairs(migrated) do
		if not sdone[r.exa_schema] then sdone[r.exa_schema] = true; add('CREATE SCHEMA IF NOT EXISTS ' .. qi(r.exa_schema) .. ';') end
	end

	add('-- ### TABLES (dropped and recreated - re-running replaces the tables) ###')
	for _, r in ipairs(migrated) do
		add('DROP TABLE IF EXISTS ' .. qi(r.exa_schema) .. '.' .. qi(r.exa_table) .. ' CASCADE CONSTRAINTS;')
		add('CREATE TABLE ' .. qi(r.exa_schema) .. '.' .. qi(r.exa_table) .. ' (' .. table.concat(r.defs, ', ') .. ');')
	end

	local pk_lines = {}
	for _, r in ipairs(migrated) do if r.pk_line then pk_lines[#pk_lines + 1] = r.pk_line end end
	if #pk_lines > 0 then add('-- ### PRIMARY KEYS (DISABLED) ###'); for _, l in ipairs(pk_lines) do add(l) end end

	if #FK_OUT > 0 then
		add('-- ### FOREIGN KEYS (DISABLED) ###')
		for _, f in ipairs(FK_OUT) do
			local c_, p_ = REL_BY_OID[f.coid], REL_BY_OID[f.poid]
			local cc, pc = {}, {}
			for _, p in ipairs(f.pairs) do cc[#cc + 1] = qi(c_.colmap[p.ccol].exa); pc[#pc + 1] = qi(p_.colmap[p.pcol].exa) end
			add('ALTER TABLE ' .. qi(c_.exa_schema) .. '.' .. qi(c_.exa_table) .. ' ADD CONSTRAINT ' .. qi(f.exa_name) .. ' FOREIGN KEY (' ..
			    table.concat(cc, ', ') .. ') REFERENCES ' .. qi(p_.exa_schema) .. '.' .. qi(p_.exa_table) .. ' (' .. table.concat(pc, ', ') .. ') DISABLE;')
		end
	end

	if #PARTS > 0 then add('-- ### PARTITION BY ###'); for _, l in ipairs(PARTS) do add(l) end end
	if #COMMENTS > 0 then add('-- ### COMMENTS ###'); for _, c in ipairs(COMMENTS) do add(c[4]) end end

	-- TIME ZONE block + IMPORTs (not needed when no table has an IMPORT, e.g. only unpopulated materialized views)
	local any_import = false
	for _, r in ipairs(migrated) do if r.import_row then any_import = true end end
	if any_import then add('-- ##########################################################################################' .. NL ..
	    '-- !!! IMPORTANT - TIME ZONE !!!' .. NL ..
	    '-- !!! Run the next ALTER SESSION and ALL IMPORT statements below in the SAME session and in' .. NL ..
	    '-- !!! this order. timestamptz values are transferred as UTC and are stored correctly ONLY' .. NL ..
	    '-- !!! while the session TIME_ZONE is ' .. SQ .. 'UTC' .. SQ .. '. If you run an IMPORT on its own, execute' .. NL ..
	    '-- !!! ALTER SESSION SET TIME_ZONE = ' .. SQ .. 'UTC' .. SQ .. '; first. The original time zone is restored at the end.' .. NL ..
	    '-- ##########################################################################################')
	add('ALTER SESSION SET TIME_ZONE = ' .. sl('UTC') .. ';')
	add('-- ### IMPORTS ###') end
	for _, r in ipairs(migrated) do
		if r.kind == 'm' and not r.populated then
			add('-- ' .. oneline(r.exa_schema) .. '.' .. oneline(r.exa_table) .. ' - materialized view is not populated, no IMPORT')
		else
			add(r.import_row)
		end
	end

	-- CONSTRAINT STATE
	if CSTATE ~= 'FORCE_DISABLE' then
		local lines = {}
		for _, r in ipairs(migrated) do
			if r.pk_name then lines[#lines + 1] = 'ALTER TABLE ' .. qi(r.exa_schema) .. '.' .. qi(r.exa_table) .. ' MODIFY CONSTRAINT ' .. qi(r.pk_name) .. ' ENABLE;' end
		end
		for _, f in ipairs(FK_OUT) do
			local c_ = REL_BY_OID[f.coid]
			if CSTATE == 'FORCE_ENABLE' or (f.valid and f.enforced) then
				lines[#lines + 1] = 'ALTER TABLE ' .. qi(c_.exa_schema) .. '.' .. qi(c_.exa_table) .. ' MODIFY CONSTRAINT ' .. qi(f.exa_name) .. ' ENABLE;'
			else
				lines[#lines + 1] = '-- ' .. oneline(c_.exa_schema) .. '.' .. oneline(c_.exa_table) .. ' foreign key ' .. oneline(f.exa_name) .. ' stays DISABLED (NOT VALID or NOT ENFORCED in PostgreSQL)'
			end
		end
		if #lines > 0 then
			add('-- ### CONSTRAINT STATE - run after the IMPORTs (' .. CSTATE .. ') ###')
			for _, l in ipairs(lines) do add(l) end
		end
	end

	-- DATA VALIDATION
	if G_CHECK then
		-- summary tables of an older release (other column layout) are replaced, otherwise the INSERTs would fail
		local EXPECT = {TABLE_NAME = 'VARCHAR(256) UTF8', METRIC = 'VARCHAR(300) UTF8', EXASOL_METRIC = 'VARCHAR(2000000) UTF8',
		                POSTGRES_METRIC = 'VARCHAR(2000000) UTF8', STATUS = 'VARCHAR(10) ASCII'}
		local OLD_LAYOUT = {}
		do
			local cols = {}
			local ok, q = pquery('select column_table, column_name, column_type from exa_all_columns where column_schema = ' .. sl(SCRIPT_SCHEMA) ..
			                     ' and right(column_table, 8) = ' .. sl('_MIG_CHK'))
			if ok then
				for i = 1, #q do
					local t = q[i][1]
					cols[t] = cols[t] or {n = 0, bad = false}
					cols[t].n = cols[t].n + 1
					if EXPECT[q[i][2]] ~= q[i][3] then cols[t].bad = true end
				end
				for t, v in pairs(cols) do if v.bad or v.n ~= 5 then OLD_LAYOUT[t] = true end end
			end
		end
		local sums_done, sums_list = {}, {}
		for _, r in ipairs(migrated) do
			if not (r.kind == 'm' and not r.populated) then
				if #sums_list == 0 and next(sums_done) == nil then
					add('-- ### DATA VALIDATION (CHECK_MIGRATION) - compares source and target metrics; run after the IMPORTs in the same session (TIME_ZONE ' .. SQ .. 'UTC' .. SQ .. ', set again below so that this section can be re-run on its own) ###')
					add('ALTER SESSION SET TIME_ZONE = ' .. sl('UTC') .. ';')
				end
				local sname = r.exa_schema
				if ulen(sname) > 120 then sname = usub(sname, 120) end
				local summary = qi(SCRIPT_SCHEMA) .. '.' .. qi(sname .. '_MIG_CHK')
				if not sums_done[summary] then
					sums_done[summary] = true
					sums_list[#sums_list + 1] = summary
					if OLD_LAYOUT[sname .. '_MIG_CHK'] then
						add('-- the summary table ' .. oneline(summary) .. ' has the layout of an older release and is replaced')
						add('DROP TABLE IF EXISTS ' .. summary .. ';')
					end
					add('CREATE TABLE IF NOT EXISTS ' .. summary .. ' ("TABLE_NAME" VARCHAR(256) UTF8, "METRIC" VARCHAR(300) UTF8, "EXASOL_METRIC" VARCHAR(2000000) UTF8, "POSTGRES_METRIC" VARCHAR(2000000) UTF8, "STATUS" VARCHAR(10) ASCII);')
				end
				add('DELETE FROM ' .. summary .. ' WHERE "TABLE_NAME" = ' .. sl(r.exa_table) .. ';')
				-- metrics: {name, exasol expression, postgres expression}
				local M = {{'ROW_CNT', 'cast(count(*) as decimal(36,0))', 'cast(count(*) as decimal(36,0))'}}
				for _, col in ipairs(r.cols) do
					local m = col.m
					if not m.unsupported then
						local e = qi(col.exa)
						local p = m.src
						local vc = m.typ:sub(1, 7) == 'VARCHAR'
						local pnull = vc and ('nullif((' .. p .. ')::text, ' .. SQ .. SQ .. ')') or ('(' .. p .. ')')
						if m.cls == 'tsz' then pnull = '(' .. p .. ')' end
						M[#M + 1] = {col.exa .. '_NULLS', 'cast(count(case when ' .. e .. ' is null then 1 end) as decimal(36,0))', 'cast(count(case when ' .. pnull .. ' is null then 1 end) as decimal(36,0))'}
						local distinct_ok = (m.cls == 'dec' and not m.capped) or m.cls == 'date' or m.cls == 'ts' or m.cls == 'bool' or m.cls == 'uuid' or m.cls == 'char' or m.plain_text
						if distinct_ok then
							local pd = vc and ('nullif((' .. p .. ')::text, ' .. SQ .. SQ .. ') collate "C"') or (m.cls == 'char' and ('(' .. p .. ') collate "C"') or ('(' .. p .. ')'))
							M[#M + 1] = {col.exa .. '_DISTINCT', 'cast(count(distinct ' .. e .. ') as decimal(36,0))', 'cast(count(distinct ' .. pd .. ') as decimal(36,0))'}
						end
						if m.cls == 'dec' and m.exact then
							local pc = m.chk or p
							local t = 'decimal(36,' .. istr(m.s) .. ')'
							M[#M + 1] = {col.exa .. '_MIN', 'cast(min(' .. e .. ') as ' .. t .. ')', 'cast(min(' .. pc .. ') as ' .. t .. ')'}
							M[#M + 1] = {col.exa .. '_MAX', 'cast(max(' .. e .. ') as ' .. t .. ')', 'cast(max(' .. pc .. ') as ' .. t .. ')'}
							if m.p <= 28 then M[#M + 1] = {col.exa .. '_SUM', 'cast(sum(' .. e .. ') as ' .. t .. ')', 'cast(sum(' .. pc .. ') as ' .. t .. ')'} end
						end
						if m.capped then
							M[#M + 1] = {col.exa .. '_ROUNDED', 'cast(0 as decimal(36,0))', 'cast(count(case when ' .. qi(col.name) .. "::text not in ('NaN', 'Infinity', '-Infinity') and " .. qi(col.name) .. ' <> round(' .. qi(col.name) .. ', ' .. istr(m.s) .. ') then 1 end) as decimal(36,0))'}
						end
						if m.cls == 'date' or m.cls == 'ts' then
							M[#M + 1] = {col.exa .. '_MIN', 'min(' .. e .. ')', 'min(' .. p .. ')'}
							M[#M + 1] = {col.exa .. '_MAX', 'max(' .. e .. ')', 'max(' .. p .. ')'}
						end
						if m.cls == 'tsz' then
							M[#M + 1] = {col.exa .. '_MIN_UTC', 'to_char(min(' .. e .. '), ' .. sl('YYYY-MM-DD HH24:MI:SS.FF6') .. ')', 'to_char(min(' .. p .. '), ' .. sl('YYYY-MM-DD HH24:MI:SS.US') .. ')'}
							M[#M + 1] = {col.exa .. '_MAX_UTC', 'to_char(max(' .. e .. '), ' .. sl('YYYY-MM-DD HH24:MI:SS.FF6') .. ')', 'to_char(max(' .. p .. '), ' .. sl('YYYY-MM-DD HH24:MI:SS.US') .. ')'}
						end
						if m.plain_text then
							M[#M + 1] = {col.exa .. '_MINLEN', 'cast(min(length(' .. e .. ')) as decimal(36,0))', 'cast(min(length(' .. pnull .. ')) as decimal(36,0))'}
							M[#M + 1] = {col.exa .. '_MAXLEN', 'cast(max(length(' .. e .. ')) as decimal(36,0))', 'cast(max(length(' .. pnull .. ')) as decimal(36,0))'}
						end
						if m.json then
							M[#M + 1] = {col.exa .. '_INVALID_JSON', 'cast(count(case when ' .. e .. ' is not null and ' .. e .. ' is not json then 1 end) as decimal(36,0))', 'cast(0 as decimal(36,0))'}
						end
					end
				end
				-- chunks: at most 250 metrics and about 90 KB per statement
				local chunk, size = {}, 0
				local function flush()
					if #chunk == 0 then return end
					local ex, px, en, ec, pc = {}, {}, {}, {}, {}
					for i, mm in ipairs(chunk) do
						local a = '"M' .. istr(i) .. '"'
						ex[#ex + 1] = mm[2] .. ' as ' .. a
						px[#px + 1] = mm[3] .. ' as ' .. a
						en[#en + 1] = 'when ' .. istr(i) .. ' then ' .. sl(mm[1])
						ec[#ec + 1] = 'when ' .. istr(i) .. ' then cast(e.' .. a .. ' as varchar(2000000))'
						pc[#pc + 1] = 'when ' .. istr(i) .. ' then cast(p.' .. a .. ' as varchar(2000000))'
					end
					local pgsel = 'select ' .. table.concat(px, ', ') .. from_clause(r)
					add('INSERT INTO ' .. summary .. ' ("TABLE_NAME", "METRIC", "EXASOL_METRIC", "POSTGRES_METRIC", "STATUS") select ' .. sl(r.exa_table) ..
					    ', v.metric, v.ev, v.pv, case when coalesce(v.ev, ' .. sl('~NULL~') .. ') = coalesce(v.pv, ' .. sl('~NULL~') .. ') then ' .. sl('OK') .. ' else ' .. sl('DEVIATION') .. ' end' ..
					    ' from (select case k.i ' .. table.concat(en, ' ') .. ' end as metric, case k.i ' .. table.concat(ec, ' ') .. ' end as ev, case k.i ' .. table.concat(pc, ' ') .. ' end as pv' ..
					    ' from (select ' .. table.concat(ex, ', ') .. ' from ' .. qi(r.exa_schema) .. '.' .. qi(r.exa_table) .. ') e' ..
					    ' cross join (select * from (import from jdbc at ' .. CONN .. ' statement ' .. sl(pgsel) .. ')) p' ..
					    ' cross join (select level as i from sys.dual connect by level <= ' .. istr(#chunk) .. ') k) v;')
					chunk, size = {}, 0
				end
				for _, mm in ipairs(M) do
					local s = #mm[2] + #mm[3] + 2 * #mm[1] + 120
					if #chunk >= 250 or size + s > 90000 then flush() end
					chunk[#chunk + 1] = mm; size = size + s
				end
				flush()
			end
		end
		for _, summary in ipairs(sums_list) do
			add('-- review deviations with - select * from ' .. oneline(summary) .. ' where "STATUS" = ' .. sl('DEVIATION') .. ' order by "TABLE_NAME", "METRIC";')
		end
	end

	add('-- restore the time zone of the session that generated this script')
	add('ALTER SESSION SET TIME_ZONE = ' .. sl(SESSION_TZ) .. ';')
end

-------------------------------------------------------------------------------------------------------------
-- VIEW review section
-------------------------------------------------------------------------------------------------------------
-- views, and with MIGRATE_MATERIALIZED_VIEWS the definitions of the migrated materialized views (refresh logic)
if G_VIEWS or G_MV then
	local DEF_CAP = 1990000
	local vq = [=[select c.oid::bigint, c.relkind::text, regexp_replace(n.nspname || '.' || c.relname, '[[:cntrl:]]', ' ', 'g'),
		left(regexp_replace(regexp_replace(pg_get_viewdef(c.oid, true), '[' || chr(1) || '-' || chr(8) || chr(11) || chr(12) || chr(14) || '-' || chr(31) || chr(127) || ']', ' ', 'g'),
			chr(13) || chr(10) || '|' || chr(13) || '|' || chr(10), chr(10) || '-- ', 'g'), ]=] .. istr(DEF_CAP) .. [=[)
		from pg_class c join pg_namespace n on n.oid = c.relnamespace
		where c.relkind in (]=] .. (G_VIEWS and "'v', 'm'" or "'m'") .. ') and' .. RELFILTER .. [[ order by 3]]
	local qr = remote(vq)
	if #qr > 0 then
		add(G_VIEWS and '-- ### VIEWS (PostgreSQL definitions - commented out, review and adapt to Exasol SQL manually) ###'
		             or '-- ### MATERIALIZED VIEW DEFINITIONS (PostgreSQL - commented out, to rebuild the refresh logic) ###')
		for i = 1, #qr do
			local oid, kind, label, def = int(qr[i][1]), qr[i][2], qr[i][3], qr[i][4]
			if isnull(def) then def = '' end
			local head = (kind == 'm') and ((G_MV and 'MATERIALIZED VIEW (migrated as a table) ' or 'MATERIALIZED VIEW (not migrated) ') .. label) or ('VIEW ' .. label)
			local extra = ''
			for _, c in ipairs(VIEW_COMMENTS[oid] or {}) do
				local piece = NL .. '-- ' .. cmt(c)
				if ulen(extra) + ulen(piece) > 100000 then extra = extra .. NL .. '-- (further comments omitted)' break end
				extra = extra .. piece
			end
			-- one output row holds 2,000,000 characters: head + comments + definition must fit
			local room = DEF_CAP - ulen(head) - ulen(extra) - 100
			local truncated = ulen(def) >= DEF_CAP
			if ulen(def) > room then def = usub(def, math.max(room, 0)); truncated = true end
			if truncated then
				-- only a partially cut comment prefix at the very end must go
				local changed = true
				while changed do
					changed = false
					for _, tail in ipairs({NL .. '--', NL .. '-', NL}) do
						if def:sub(-#tail) == tail then def = def:sub(1, #def - #tail); changed = true end
					end
				end
				def = def .. NL .. '-- (definition truncated)'
			end
			add('-- ' .. cmt(head) .. extra .. NL .. '-- ' .. def)
		end
	end
end

-- EXAplus replaces &name in statements (DEFINE) unless SET DEFINE OFF is run first
do
	for i = 4, #OUT do
		local row = OUT[i][1]
		if row:sub(1, 2) ~= '--' and row:find('&', 1, true) then
			table.insert(OUT, 4, {'-- !!! EXAPLUS: some statements contain the character & - in EXAplus run SET DEFINE OFF; before this script, otherwise & starts a substitution variable and the text is changed'})
			break
		end
	end
end

if #RELS == 0 then add('-- no table matched SCHEMA_FILTER / TABLE_FILTER (LIKE patterns, case-sensitive)' .. (LONE_PARTS and ' - see the PARTITION FILTER note' or ''))
elseif #migrated == 0 then add('-- no table is migrated: the filters matched only views or relations that are not migrated (see the notes' .. (G_VIEWS and ' and the VIEWS section)' or ')')) end
-- safety net: a row longer than the output column would end the session without any output
for i = 1, #OUT do
	local row = OUT[i][1]
	if #row > 1990000 and ulen(row) > 1990000 then
		OUT[i] = {'-- !!! SIZE LIMIT: a generated row of ' .. istr(ulen(row)) .. ' characters exceeds the output limit of 2,000,000 characters and is omitted; it starts with: ' .. oneline(usub(row, 200))}
	end
end
exit(OUT, 'SQL_TEXT VARCHAR(2000000)')
/

-- ===================================================================================================
-- CONNECTION SETUP
-- ===================================================================================================
-- Prerequisites
--   * The PostgreSQL database (version 12 or newer) must be reachable from this Exasol database.
--   * The connection user must be able to read all tables to migrate. Row-level security filters rows
--     silently - use a user with BYPASSRLS, or add  options=-c%20row_security%3Doff  to the JDBC URL so
--     that filtering makes the IMPORT fail instead.
--   * Use the latest PostgreSQL JDBC driver (postgresql), 42.7.11 or higher.
--
-- JDBC driver (install once in BucketFS - the driver and its settings.cfg)
--   * https://mvnrepository.com/artifact/org.postgresql/postgresql
--   * Driver setup guide - https://docs.exasol.com/db/latest/loading_data/connect_sources/postgresql.htm
--
-- Create a connection to the PostgreSQL database (adjust host, database name and credentials),
-- then run the accompanying test query.

CREATE OR REPLACE CONNECTION POSTGRESQL_JDBC
    TO 'jdbc:postgresql://postgresql_host_or_ip:5432/my_database'
    USER 'username' IDENTIFIED BY 'password';
SELECT * FROM (IMPORT FROM JDBC AT POSTGRESQL_JDBC STATEMENT 'SELECT ''Connection works''');

-- ===================================================================================================
-- GENERATE THE MIGRATION STATEMENTS (recommended defaults shown)
-- ===================================================================================================
EXECUTE SCRIPT DATABASE_MIGRATION.POSTGRESQL_TO_EXASOL(
    'POSTGRESQL_JDBC',  -- CONNECTION_NAME: name of the JDBC connection created above
    true,               -- IDENTIFIER_CASE_INSENSITIVE: true (recommended) => fold all identifiers to UPPER case (PostgreSQL folds unquoted names to lower case, so nothing is lost); false => keep them as in PostgreSQL (quoted)
    '%',                -- SCHEMA_FILTER: source schema(s) as a LIKE pattern, e.g. 'public', 'sales%', '%' (all; system schemas always excluded). '_' is a wildcard too
    '%',                -- TABLE_FILTER: table(s) as a LIKE pattern, e.g. 'orders', 'fact%', '%' (all)
    '',                 -- TARGET_SCHEMA: Exasol target schema; '' (recommended) => use the source schema name
    'AUTO',             -- PARALLEL_STATEMENTS: 'AUTO' (recommended; Exasol VCPU/NODES/2, even, 4..64, at most half of the free PostgreSQL connections), a number >= 1, or 1 = no parallel reading. Parallel reading needs PostgreSQL 14+; each STATEMENT is a separate transaction - do not write to the source during the load
    1000000,            -- PARALLEL_MIN_ROWS: tables with fewer estimated rows are read with one STATEMENT (default 1000000; splitting smaller tables costs more than it saves); 0 => always split
    'FORCE_DISABLE',    -- CONSTRAINT_STATE: 'FORCE_DISABLE' (recommended; keys stay metadata for the optimizer and BI tools), 'SET_AS_SOURCE' (enable keys that are valid and enforced in PostgreSQL) or 'FORCE_ENABLE' (Exasol validates all keys)
    true,               -- GENERATE_COMMENTS: true (recommended) => migrate schema, table and column comments
    true,               -- GENERATE_VIEWS: true => list the source views as a commented manual-review section
    false,              -- MIGRATE_MATERIALIZED_VIEWS: false (default) => materialized views are only listed for review; true => migrate them as tables with their current content (a snapshot - Exasol does not refresh it)
    true,               -- GENERATE_PARTITION_BY: true => best-effort PARTITION BY from a single-column PostgreSQL partition key
    'BASE64',           -- BINARY_HANDLING: 'BASE64' (recommended; bytea as base64 text, max 1,500,000 bytes) or 'SKIP' (load NULL)
    'CAP',              -- DECIMAL_OVERFLOW: 'CAP' (recommended; numeric(p > 36, s) -> DECIMAL(36, s') keeping the integer digits and rounding surplus fractional digits, unconstrained numeric -> DECIMAL(36,18) = at most 18 integer digits; larger values fail), 'DOUBLE' (nearest double, about 15 digits) or 'VARCHAR' (lossless text)
    false,              -- TRUNCATE_LONG_STRINGS: false (recommended) => the IMPORT fails on a value > 2,000,000 characters; true => cut such values (json/array text may become invalid)
    'VARCHAR',          -- INTERVAL_HANDLING: 'VARCHAR' (recommended; lossless text) or 'INTERVAL' (native INTERVAL DAY TO SECOND(3) - millisecond precision, best-effort; month/year intervals fail)
    'FAIL',             -- TEMPORAL_OUT_OF_RANGE: 'FAIL' (recommended; the IMPORT fails on a value outside the Exasol range, incl. infinity and BC), 'NULL' (load NULL) or 'CLAMP' (clamp to the Exasol min/max)
    false               -- CHECK_MIGRATION: true => also generate the data validation (summary table <schema>_MIG_CHK in the script schema)
);
