create schema if not exists database_migration;

/*
    sqlserver_to_exasol.sql  -  generate the statements to migrate Microsoft SQL Server or Azure SQL databases to Exasol v8.

    Source: SQL Server 2017, 2019, 2022 and 2025 (tested), SQL Server 2016 best effort (out of extended support since
    2026-07-14), Azure SQL Managed Instance and Azure SQL Database (one database per connection). Use the latest
    Microsoft JDBC driver (mssql-jdbc 13.6.0 or newer), not jTDS.
    This script runs on the TARGET Exasol database, reads the SOURCE metadata through a JDBC connection and RETURNS the
    statements to recreate and load the source. It changes nothing itself - review the output and run it in the order
    returned, in ONE session (see TIME ZONE below), preferably with stop-on-error (EXAplus option -x; EXAplus continues
    after errors by default). In EXAplus run SET DEFINE OFF; first when the output contains the character & (a note says
    so), otherwise EXAplus treats &name as a substitution variable.

    OUTPUT (in this order): header and notes, CREATE SCHEMA, DROP TABLE IF EXISTS ... CASCADE CONSTRAINTS + CREATE TABLE,
    PRIMARY KEYs and FOREIGN KEYs (created DISABLED), PARTITION BY, COMMENTs, the TIME ZONE block, the IMPORTs, the
    CONSTRAINT STATE section (SET_AS_SOURCE / FORCE_ENABLE), the optional DATA VALIDATION (CHECK_MIGRATION), the restore of
    the session time zone, and the optional commented VIEW review section. Re-running the output replaces the target
    tables (DROP ... CASCADE CONSTRAINTS also drops foreign keys that OTHER Exasol tables hold on them - a FOREIGN KEYS
    DROPPED note lists them). TARGET_SCHEMA in double quotes is used verbatim (no upper-casing); at most 128 characters.

    TIME ZONE (important): datetimeoffset is migrated as TIMESTAMP(n) WITH LOCAL TIME ZONE: the instant is kept exactly,
    the original offset (+02:00 ...) is not. The values are transferred as UTC. Exasol interprets incoming values in the
    SESSION time zone, so the output switches the session to TIME_ZONE = 'UTC' before the IMPORTs and restores the
    original zone at the end. Run the ALTER SESSION and the IMPORTs in the SAME session. Afterwards every session sees the
    values in its own time zone. If the run stops at an error before the end, the session stays in UTC - the banner in
    the output names the ALTER SESSION statement that restores the original zone.
    The usable datetimeoffset range is 0001-01-02 .. 9999-12-30 UTC (values on the outermost day cannot be displayed in
    every session time zone); TEMPORAL_OUT_OF_RANGE applies to values outside it: 'FAIL' (the IMPORT fails and names the
    column and the value), 'NULL' (load NULL) or 'CLAMP' (load the nearest value of the range, e.g. for a 9999-12-31
    "open end"). date, datetime, datetime2 and smalldatetime have no time zone; their full range (up to 9999-12-31) is
    transferred unchanged (except the 1582 gap, see KNOWN LIMITS).

    DATA TYPE MAPPING (by the base system type - alias types resolve to their base type):
      bit -> DECIMAL(1,0); tinyint/smallint/int/bigint -> DECIMAL(3/5/10/19,0); decimal/numeric(p <= 36, s) ->
      DECIMAL(p,s); money/smallmoney -> DECIMAL(19,4)/(10,4); float/real -> DOUBLE; date -> DATE; datetime ->
      TIMESTAMP(3); smalldatetime -> TIMESTAMP(0); datetime2(n) -> TIMESTAMP(n); datetimeoffset(n) -> TIMESTAMP(n) WITH
      LOCAL TIME ZONE; char/nchar(n <= 2000) -> CHAR(n) UTF8; varchar/nvarchar/text/ntext/char(n > 2000) -> VARCHAR UTF8
      (nvarchar(n) -> VARCHAR(n): the length is in characters); sysname -> VARCHAR(128); uniqueidentifier -> CHAR(36).
      Non-UTF-8 code-page columns (char/varchar/text) are converted by SQL Server itself (CAST AS NVARCHAR), so every
      byte arrives as SQL Server shows it.
      Small documented differences: time(n) -> VARCHAR(16) ('HH:MI:SS[.fffffff]', Exasol has no TIME type);
      binary(n <= 1024) and rowversion -> HASHTYPE, other binary/varbinary/image -> hex text (BINARY_HANDLING; a binary value
      holds at most 1,000,000 bytes); xml/json/vector -> VARCHAR text (vector elements keep 8 significant digits);
      sql_variant -> VARCHAR text of its value (numbers with full precision, date/time in ISO form incl. the offset; the base
      type is not kept); hierarchyid -> VARCHAR ('/1/2/'); geometry/geography -> GEOMETRY (2-D WKT: Z/M values and SRIDs are
      not kept, curves become line approximations - invalid curved instances are made valid first -, a geography FULLGLOBE
      becomes NULL); float: normal values arrive exactly, -0.0 becomes 0, subnormal values (|x| < 2.2250738585072014e-308) may
      become 0, |x| > 1.7976e308 makes the IMPORT fail.
      Exasol stores an empty string as NULL: empty character, binary and text values become NULL. NOT NULL is therefore
      only kept on numeric, date, timestamp, hashtype and uniqueidentifier targets - and not on datetimeoffset under
      TEMPORAL_OUT_OF_RANGE='NULL'.
      Identity columns are migrated as plain columns carrying their values (no IDENTITY in Exasol - inserts must supply
      the value). Computed columns are migrated as plain columns with their current values. Columns named LEVEL, ROWNUM,
      ROWID, CONNECT_BY_ISLEAF or CONNECT_BY_ISCYCLE (not allowed as Exasol column names) get a trailing underscore.
      DEFAULTs are migrated when Exasol can represent them with the same meaning (literals, getdate/sysdatetime ->
      CURRENT_TIMESTAMP, getutcdate/sysutcdatetime -> the UTC time); others are listed as notes.
      Hard limits (the IMPORT fails loudly rather than corrupting data): a value > 2,000,000 characters (unless
      TRUNCATE_LONG_STRINGS=true), a binary value > 1,000,000 bytes, a decimal value with more integer digits than its
      DECIMAL_OVERFLOW='CAP' column holds (decimal(p > 36, s) -> DECIMAL(36, s) with 36 - s integer digits, e.g.
      decimal(38,2) -> DECIMAL(36,2); a scale above 35 is rounded to 35 so that one integer digit remains, so a value that
      rounds up to the next integer digit, e.g. 9.99...9 in decimal(38,37), fails too; a DECIMAL CAP note names such columns),
      a datetimeoffset value outside 0001-01-02 .. 9999-12-30 UTC under TEMPORAL_OUT_OF_RANGE='FAIL'.

    PARALLEL IMPORT: a table with at least PARALLEL_MIN_ROWS rows is read by up to PARALLEL_STATEMENTS parallel
    STATEMENT clauses, each reading a disjoint part (exact 1:1): partitions of a partitioned table, key ranges of the
    clustered index (boundaries from the statistics histogram) or physical row locations of a heap / columnstore table -
    also of a clustered table whose histogram gives very uneven key ranges (e.g. a sequential uniqueidentifier key). A note
    names tables that get fewer or uneven STATEMENTs (few partitions or distinct key values, key values very close together).
    AUTO = half the vCPUs of one Exasol node (VCPU/NODES/2), even, 4..64 - the rule of postgresql_to_exasol.sql and
    snowflake_to_exasol.sql -, at most the SQL Server processor count. Each STATEMENT is a separate SQL Server transaction
    - the source must not be written during the load; for a consistent copy migrate from a database snapshot (CREATE
    DATABASE ... AS SNAPSHOT OF ..., then DB_FILTER = the snapshot name and TARGET_SCHEMA set; not on Azure SQL Database).
    Each stream needs temporary memory in Exasol: on nodes with little memory use fewer streams for very large tables.

    CONSTRAINTS: PK/FK are always created DISABLED (the primary key is named <TABLE>_PK, foreign keys keep their SQL Server
    names); a final CONSTRAINT STATE section enables them according to
    CONSTRAINT_STATE (SET_AS_SOURCE keeps keys that are disabled or not trusted - WITH NOCHECK - disabled). A foreign key
    is migrated when its parent table is migrated and it references the parent primary key. Exasol compares strings
    binary: keys that hold only under a case-insensitive collation or trailing-blank padding may fail to enable.

    SPECIAL TABLES (listed as notes): row-level security and dynamic data masking filter or mask the migrated data for a
    connection user without exemption / UNMASK (CHECK_MIGRATION then reports DEVIATIONs for masked columns, because SQL
    Server masks the source aggregates too); Always Encrypted columns are not migrated; graph tables (internal columns skipped), memory-optimized tables (read
    WITH (SNAPSHOT)), temporal history and ledger tables (migrated as plain tables), tables with a disabled clustered
    index (created empty), indexed views (only with MIGRATE_INDEXED_VIEWS). System databases are migrated only when
    DB_FILTER names them exactly; Microsoft-shipped objects, external tables and temporary tables are never migrated.
    Not migrated: indexes, UNIQUE/CHECK constraints, sequences, synonyms, rules, functions/procedures/triggers, users,
    roles and permissions.

    PRIVILEGES: the metadata and the data are read through the connection user: grant VIEW DEFINITION (otherwise DEFAULTs,
    view definitions and security policies are invisible, and tables the user may not SELECT - or in a schema hidden by a
    schema-level DENY VIEW DEFINITION, even with SELECT granted - are missing from the output
    - a note says so) and SELECT on the tables (a table without SELECT or with a column-level DENY is skipped with a note;
    a database whose catalog the user may not read is skipped with a note). On the Exasol side, the user running the
    generator and the output needs the system privilege IMPORT and the connection granted with GRANT CONNECTION (ACCESS ON
    CONNECTION is not enough), CREATE SCHEMA / CREATE TABLE for the target, and for CHECK_MIGRATION the right to create
    tables in the script schema (owner of that schema, or CREATE ANY TABLE and DROP ANY TABLE).

    KNOWN LIMITS: dates between 1582-10-05 and 1582-10-14 arrive shifted by 10 days (Julian/Gregorian gap of the transfer;
    a date key may then collide with real 1582-10-15..24 values); a STATEMENT of 131,072 bytes or more is rejected by
    Exasol (a SIZE LIMIT note names such very wide tables); a table with more than 4,096 columns is not migrated. A primary key with an empty string value cannot be enabled ('' is NULL
    in Exasol). Filters: comma separated names or LIKE patterns, as in postgresql_to_exasol.sql (SCHEMA_FILTER and
    TABLE_FILTER: '', NULL and '%' select all; DB_FILTER must not be empty; a list of only commas is an error); _ and %
    are always wildcards (e.g. 'sales_2024' also matches salesX2024), [ and ] are literal characters; case sensitivity follows the source collation (e.g. under a Turkish collation 'KISI' does not
    match Kisi). A filter that names a view, synonym, sequence or table type matches no table.
    CHECK_MIGRATION reads every source table again - once per group of up to 250 metrics, and the distinct count of every
    column adds a scan of its own - so on wide tables the check can take much longer than the IMPORT. It compares the
    transported values: under TEMPORAL_OUT_OF_RANGE = 'NULL' or 'CLAMP' the changed datetimeoffset values are not counted. The per-table
    <table>_MIG_CHK tables of the previous release in the target schemas are not removed.
    Comments and DEFAULT texts are copied from the source as they are: when they contain a question mark, a colon before a
    quote or a backslash before a quote, a client such as DbVisualizer may ask for parameter values when running the
    output - disable its parameter / variable substitution for that run.
*/
--/
create or replace script database_migration.SQLSERVER_TO_EXASOL(
  CONNECTION_NAME               -- name of the JDBC connection inside Exasol, e.g. SQLSERVER_JDBC
  ,DB2SCHEMA                    -- false (recommended) => "schema"."table"; true => "database"."schema_table" (several databases at once)
  ,DB_FILTER                    -- SQL Server database(s): a name or LIKE pattern, or a comma list of them, e.g. 'sales', 'db1, db2', 'dwh%'
  ,SCHEMA_FILTER                -- schema(s): names or LIKE patterns, comma separated, '%' = all
  ,TARGET_SCHEMA                -- target schema on Exasol; '' = use the source schema (or, with DB2SCHEMA, the database) name
  ,TABLE_FILTER                 -- table(s): names or LIKE patterns, comma separated, '%' = all
  ,IDENTIFIER_CASE_INSENSITIVE  -- true (recommended) => fold all identifiers to UPPER case; false => keep them as in SQL Server
  ,PARALLEL_STATEMENTS          -- 'AUTO' (recommended: Exasol VCPU/NODES/2, even, 4..64, at most the SQL Server processor count), a number >= 1, or 1 = one STATEMENT per table
  ,PARALLEL_MIN_ROWS            -- tables with fewer rows are read with one STATEMENT (default 1000000); 0 = split every table that can be split
  ,CONSTRAINT_STATE             -- 'FORCE_DISABLE' (recommended), 'SET_AS_SOURCE' or 'FORCE_ENABLE'
  ,GENERATE_COMMENTS            -- true/false: migrate MS_Description comments of schemas, tables and columns
  ,GENERATE_VIEWS               -- true/false: list the source views as a commented manual-review section
  ,MIGRATE_INDEXED_VIEWS        -- false (default) => indexed views are only listed for review; true => migrate their stored rows as tables
  ,GENERATE_PARTITION_BY        -- true/false: PARTITION BY from the SQL Server partitioning column (when its Exasol type allows it)
  ,BINARY_HANDLING              -- 'HASHTYPE' (recommended; binary(n <= 1024) and rowversion -> HASHTYPE, other binary -> hex text), 'HEX' (all binary as hex text) or 'SKIP' (binary columns are not migrated)
  ,DECIMAL_OVERFLOW             -- 'CAP' (recommended), 'DOUBLE' or 'VARCHAR' for decimal/numeric with precision > 36
  ,TRUNCATE_LONG_STRINGS        -- false (recommended): a value > 2,000,000 characters makes the IMPORT fail; true: cut it to 2,000,000 characters
  ,TEMPORAL_OUT_OF_RANGE        -- 'FAIL' (recommended), 'NULL' or 'CLAMP' for datetimeoffset values outside the usable range 0001-01-02 .. 9999-12-30 UTC
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
local BSL = string.char(92)         -- backslash

local function fail(msg) error('SQLSERVER_TO_EXASOL: ' .. msg) end
local function isnull(v) return v == nil or v == null end
local function str(v) if isnull(v) then return nil end return tostring(v) end
local function int(v) if isnull(v) then return nil end return math.floor(tonumber(v)) end
local function istr(n) return string.format('%d', math.floor(n)) end
local function trim(s) return (s:gsub('^%s+', ''):gsub('%s+$', '')) end
local function sl(s) return SQ .. (s:gsub(SQ, SQ .. SQ)) .. SQ end                 -- SQL string literal (Exasol and T-SQL)
local function nl_(s) return 'N' .. sl(s) end                                      -- T-SQL nvarchar literal
local function qi(s) return DQ .. (s:gsub(DQ, DQ .. DQ)) .. DQ end                 -- Exasol quoted identifier
local function qb(s) return '[' .. (s:gsub('%]', ']]')) .. ']' end                 -- T-SQL bracketed identifier
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
local C0 = '[' .. string.char(0) .. '-' .. string.char(8) .. string.char(11, 12) .. string.char(14) .. '-' .. string.char(31) .. string.char(127) .. ']'
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

local CONN = str(CONNECTION_NAME)
if CONN == nil or trim(CONN) == '' then fail('invalid value (empty) for CONNECTION_NAME - valid: the name of an existing JDBC connection') end
CONN = trim(CONN)
if not (CONN:match('^[%a_][%w_]*$') or (CONN:match('^' .. DQ .. '[^' .. DQ .. ']+' .. DQ .. '$'))) then
	fail('invalid value ' .. CONN .. ' for CONNECTION_NAME - valid: the name of an existing CONNECTION object (an identifier, not a JDBC URL)')
end
if type(CONNECTION_NAME) ~= 'string' then fail('invalid value ' .. CONN .. ' for CONNECTION_NAME - valid: the name of an existing CONNECTION object (an identifier, not a JDBC URL)') end
if not CONN:match('^' .. DQ) and query('select count(*) from EXA_SQL_KEYWORDS where RESERVED and KEYWORD = upper(' .. sl(CONN) .. ')')[1][1] > 0 then
	fail('invalid value ' .. CONN .. ' for CONNECTION_NAME - a reserved word must be given in the quoted form ' .. DQ .. CONN:upper() .. DQ)
end
local D2S      = p_bool(DB2SCHEMA, 'DB2SCHEMA', false)
local DBF      = str(DB_FILTER)
if DBF == nil or trim(DBF) == '' then fail('invalid value (empty) for DB_FILTER - valid: a database name or LIKE pattern, or a comma list of them') end
local SF       = str(SCHEMA_FILTER);  if SF == nil or trim(SF) == '' then SF = '%' end
local TF       = str(TABLE_FILTER);   if TF == nil or trim(TF) == '' then TF = '%' end
local TGT      = str(TARGET_SCHEMA);  if TGT ~= nil then TGT = trim(TGT); if TGT == '' then TGT = nil end end
local TGT_Q    = false
if TGT and #TGT >= 2 and TGT:sub(1, 1) == DQ and TGT:sub(-1) == DQ then
	local inner = TGT:sub(2, -2)
	if inner == '' or inner:gsub(DQ .. DQ, ''):find(DQ, 1, true) then fail('invalid value ' .. TGT .. ' for TARGET_SCHEMA - valid: a name, or a quoted name with doubled inner double quotes') end
	TGT = inner:gsub(DQ .. DQ, DQ); TGT_Q = true
end
if TGT and ulen(TGT) > 128 then fail('invalid value ' .. TGT .. ' for TARGET_SCHEMA - valid: at most 128 characters') end
local ICI      = p_bool(IDENTIFIER_CASE_INSENSITIVE, 'IDENTIFIER_CASE_INSENSITIVE', true)
local TGT_UP   = TGT and (TGT_Q and TGT or query('select upper(' .. sl(TGT) .. ') from sys.dual')[1][1]) or nil
local CSTATE   = p_opt(CONSTRAINT_STATE, 'CONSTRAINT_STATE', {'FORCE_DISABLE', 'SET_AS_SOURCE', 'FORCE_ENABLE'})
local G_COMM   = p_bool(GENERATE_COMMENTS, 'GENERATE_COMMENTS', true)
local G_VIEWS  = p_bool(GENERATE_VIEWS, 'GENERATE_VIEWS', true)
local G_IV     = p_bool(MIGRATE_INDEXED_VIEWS, 'MIGRATE_INDEXED_VIEWS', false)
local G_PART   = p_bool(GENERATE_PARTITION_BY, 'GENERATE_PARTITION_BY', true)
local BINMODE  = p_opt(BINARY_HANDLING, 'BINARY_HANDLING', {'HASHTYPE', 'HEX', 'SKIP'})
local DECOF    = p_opt(DECIMAL_OVERFLOW, 'DECIMAL_OVERFLOW', {'CAP', 'DOUBLE', 'VARCHAR'})
local TRUNC    = p_bool(TRUNCATE_LONG_STRINGS, 'TRUNCATE_LONG_STRINGS', false)
local OORMODE  = p_opt(TEMPORAL_OUT_OF_RANGE, 'TEMPORAL_OUT_OF_RANGE', {'FAIL', 'NULL', 'CLAMP'})
local G_CHECK  = p_bool(CHECK_MIGRATION, 'CHECK_MIGRATION', false)
local PS_AUTO, PS_FIX = false, 1
do
	local v = PARALLEL_STATEMENTS
	if isnull(v) or (type(v) == 'string' and (trim(v) == '' or trim(v):upper() == 'AUTO')) then
		PS_AUTO = true
	else
		local n = p_int(v)
		if n == nil or n < 1 then fail('invalid value ' .. tostring(v) .. ' for PARALLEL_STATEMENTS - valid: AUTO or an integer from 1 to 2147483647') end
		PS_FIX = math.floor(n)
	end
end
local PMIN = 1000000
do
	local v = PARALLEL_MIN_ROWS
	if not (isnull(v) or (type(v) == 'string' and trim(v) == '')) then
		local n = p_int(v)
		if n == nil or n < 0 then fail('invalid value ' .. tostring(v) .. ' for PARALLEL_MIN_ROWS - valid: an integer from 0 to 2147483647') end
		PMIN = math.floor(n)
	end
end

-- filter lists: comma separated; an element with % or _ is a LIKE pattern (SQL Server LIKE, [ escaped), else an exact name
local function filter_elems(f)
	local out = {}
	for e in (f .. ','):gmatch('([^,]*),') do
		e = trim(e)
		if e ~= '' then out[#out + 1] = e end
	end
	return out
end
local function is_pattern(e) return e:find('[%%_]') ~= nil end
local function filt(f, col)
	local parts = {}
	for _, e in ipairs(filter_elems(f)) do
		if is_pattern(e) then
			local esc = e:gsub(BSL, BSL .. BSL):gsub('%[', BSL .. '[')
			parts[#parts + 1] = col .. ' like ' .. nl_(esc) .. ' escape ' .. sl(BSL)
		else
			parts[#parts + 1] = col .. ' = ' .. nl_(e)
		end
	end
	if #parts == 0 then return '1 = 1' end
	return '(' .. table.concat(parts, ' or ') .. ')'
end
local SCH_W = filt(SF, 's.name')
local TAB_W = filt(TF, 'o.name')
if #SCH_W + #TAB_W > 40000 then fail('SCHEMA_FILTER / TABLE_FILTER too long (' .. istr(#SCH_W + #TAB_W) .. ' bytes; at most 40,000 because the filters are repeated inside the metadata queries) - use LIKE patterns or several runs') end
if #filter_elems(SF) == 0 then fail('invalid value ' .. SF .. ' for SCHEMA_FILTER - valid: names or LIKE patterns, comma separated (% = all)') end
if #filter_elems(TF) == 0 then fail('invalid value ' .. TF .. ' for TABLE_FILTER - valid: names or LIKE patterns, comma separated (% = all)') end
if #filter_elems(DBF) == 0 then fail('invalid value ' .. DBF .. ' for DB_FILTER - valid: a database name or LIKE pattern, or a comma list of them') end

-------------------------------------------------------------------------------------------------------------
-- remote (SQL Server) access
-------------------------------------------------------------------------------------------------------------
local function remote(tsql, exa_select)
	return query('select ' .. (exa_select or 't.*') .. ' from (import from jdbc at ' .. CONN .. ' statement ' .. sl(tsql) .. ') t')
end
local function premote(tsql, exa_select)
	return pquery('select ' .. (exa_select or 't.*') .. ' from (import from jdbc at ' .. CONN .. ' statement ' .. sl(tsql) .. ') t')
end

-- source environment
local env = remote([[select cast(serverproperty('ProductVersion') as nvarchar(128)) as PV, cast(serverproperty('ProductMajorVersion') as int) as PMAJ,
	cast(serverproperty('EngineEdition') as int) as EED, cast(serverproperty('Edition') as nvarchar(128)) as EDI, db_name() as CDB]])
local VER, VMAJ, EED, EDITION, CURDB = str(env[1][1]), int(env[1][2]) or 0, int(env[1][3]) or 0, str(env[1][4]), str(env[1][5])
local AZURE_DB = (EED == 5)
local AZURE = (EED == 5 or EED == 8)              -- Azure SQL Database / Managed Instance (version line 12, no ProductMajorVersion)
if VMAJ < 13 and not AZURE then fail('SQL Server 2016 or newer required (found ' .. tostring(VER) .. ')') end
local SRC_CPU = nil
do
	local ok, r = premote([[exec master.dbo.xp_msver 'ProcessorCount']])
	if ok and #r >= 1 and not isnull(r[1][3]) then SRC_CPU = int(r[1][3]) end
end

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
if PS_AUTO then
	-- same rule as snowflake_to_exasol.sql and postgresql_to_exasol.sql: half the vCPUs of one Exasol node, even, 4..64
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
	if SRC_CPU and SRC_CPU >= 1 and SRC_CPU < PS then PS = SRC_CPU; why = why .. '; capped to the SQL Server processor count ' .. istr(SRC_CPU)
	elseif not SRC_CPU then why = why .. '; SQL Server processor count not readable (xp_msver), no cap' end
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
-- databases
-------------------------------------------------------------------------------------------------------------
local DBS = {}
do
	local named_in = {}
	for _, e in ipairs(filter_elems(DBF)) do named_in[#named_in + 1] = nl_(e) end   -- named exactly = a filter element equal to the name (case-insensitive)
	if AZURE_DB then
		local r = remote('select name as N, cast(has_perms_by_name(name, ' .. sl('DATABASE') .. ', ' .. sl('VIEW DEFINITION') .. ') as int) as VD from sys.databases where name = db_name() and ' .. filt(DBF, 'name'))
		if #r == 0 then
			fail('Azure SQL Database - one connection per database: set databaseName=<database> in the connection and DB_FILTER = ' .. SQ .. tostring(CURDB) .. SQ .. ' (connected database ' .. tostring(CURDB) .. ')')
		end
		DBS[1] = {name = CURDB, vd = (int(r[1][2]) == 1)}
	else
		local r = remote('select name as N, database_id as DID, cast(state as int) as ST, state_desc as SD, cast(has_dbaccess(name) as int) as ACC, ' ..
			'cast(is_distributor as int) as DIS, case when source_database_id is null then 0 else 1 end as SNAP, ' ..
			'cast(has_perms_by_name(name, ' .. sl('DATABASE') .. ', ' .. sl('VIEW DEFINITION') .. ') as int) as VD, ' ..
			'case when name collate Latin1_General_100_CI_AS in (' .. table.concat(named_in, ', ') .. ') then 1 else 0 end as NAMED, user_access_desc as UAD ' ..
			'from sys.databases where ' .. filt(DBF, 'name') .. ' order by name')
		local sysdb = {}
		for i = 1, #r do
			local name, did, st, sd, acc, dis, snap, vd = r[i][1], int(r[i][2]), int(r[i][3]), str(r[i][4]), int(r[i][5]), int(r[i][6]), int(r[i][7]), int(r[i][8])
			local named = int(r[i][9]) == 1
			if (did <= 4 or dis == 1) and not named then
				sysdb[#sysdb + 1] = oneline(name)
			elseif snap == 1 and not named then
				note('SKIPPED DATABASE', 'database ' .. oneline(name) .. ' is a database snapshot - it is only migrated when DB_FILTER names it exactly', true)
			elseif st ~= 0 then
				note('SKIPPED DATABASE', 'database ' .. oneline(name) .. ' is not online (' .. tostring(sd) .. ') and is not migrated', true)
			elseif acc ~= 1 then
				local uad = str(r[i][10])
				note('SKIPPED DATABASE', 'database ' .. oneline(name) .. ' is not accessible for the connection user' ..
				     ((uad == 'SINGLE_USER') and ' (SINGLE_USER mode - another session holds it)' or (uad == 'RESTRICTED_USER') and ' (RESTRICTED_USER mode - only members of db_owner, dbcreator or sysadmin may connect)' or '') .. ' and is not migrated', true)
			else
				DBS[#DBS + 1] = {name = name, vd = (vd == 1)}
			end
		end
		table.sort(sysdb)
		if #sysdb > 0 then note('SKIPPED DATABASE', 'system databases matched by a pattern are not migrated (name them exactly in DB_FILTER to include them): ' .. table.concat(sysdb, ', '), true) end
	end
	table.sort(DBS, function(a, b) return a.name < b.name end)
end
local function vd_notes()
	for _, d in ipairs(DBS) do
		if not d.vd then
			note('NO VIEW DEFINITION', 'the connection user has no VIEW DEFINITION permission in database ' .. oneline(d.name) ..
			     ' - DEFAULTs, view definitions and row-level security policies are not visible and therefore not migrated or reported, and tables the user may not SELECT (or whose schema is hidden by DENY VIEW DEFINITION) are missing from this output; grant VIEW DEFINITION and SELECT for a complete migration', true)
		end
	end
end

-- one T-SQL query per database, chunked into UNION ALL statements below 100,000 bytes
local function remote_all(fn, exa_select)
	local rows, cur, len = {}, {}, 0
	local function flush()
		if #cur == 0 then return end
		local r = remote(table.concat(cur, ' union all '), exa_select)
		for i = 1, #r do rows[#rows + 1] = r[i] end
		cur, len = {}, 0
	end
	for _, d in ipairs(DBS) do
		local qs = fn(d.name, qb(d.name))
		if type(qs) ~= 'table' then qs = {qs} end
		for _, q in ipairs(qs) do
			if #cur > 0 and len + #q > 100000 then flush() end
			cur[#cur + 1] = q; len = len + #q + 11
		end
	end
	flush()
	return rows
end

local GRAPH  = (VMAJ >= 14) or AZURE
local LEDGER = (VMAJ >= 16) or AZURE
local SYS_SCHEMAS = "('sys', 'INFORMATION_SCHEMA', 'guest', 'db_owner', 'db_accessadmin', 'db_securityadmin', 'db_ddladmin', 'db_backupoperator', 'db_datareader', 'db_datawriter', 'db_denydatareader', 'db_denydatawriter')"
local OBJW = ' o.is_ms_shipped = 0 and left(o.name, 1) <> ' .. sl('#') .. ' and s.name not in ' .. SYS_SCHEMAS .. ' and ' .. SCH_W .. ' and ' .. TAB_W .. ' '
local function objfrom(B) return B .. '.sys.objects o join ' .. B .. '.sys.schemas s on s.schema_id = o.schema_id' end

-------------------------------------------------------------------------------------------------------------
-- relations (tables, and indexed views with MIGRATE_INDEXED_VIEWS)
-------------------------------------------------------------------------------------------------------------
local RELS, REL_BY = {}, {}
local function rkey(db, oid) return db .. NL .. istr(oid) end
local UPPER4 = 't.*, upper(t."DBN"), upper(t."SCH"), upper(t."TN"), upper(t."SCH" || ' .. sl('_') .. ' || t."TN")'
local function new_rel(x, kind)
	local r = {db = x[1], sch = x[2], tn = x[3], oid = int(x[4]), kind = kind, cols = {}, colmap = {}}
	local n = #x
	r.db_up, r.sch_up, r.tn_up, r.st_up = x[n - 3], x[n - 2], x[n - 1], x[n]
	if TGT then r.exa_schema = ICI and TGT_UP or TGT
	elseif D2S then r.exa_schema = exa_id(r.db, r.db_up)
	else r.exa_schema = exa_id(r.sch, r.sch_up) end
	r.exa_table = D2S and exa_id(r.sch .. '_' .. r.tn, r.st_up) or exa_id(r.tn, r.tn_up)
	r.label = oneline(r.db) .. '.' .. oneline(r.sch) .. '.' .. oneline(r.tn)
	r.src = qb(r.db) .. '.' .. qb(r.sch) .. '.' .. qb(r.tn)
	r.migrate = true
	RELS[#RELS + 1] = r
	REL_BY[rkey(r.db, r.oid)] = r
	return r
end
do
	local function rel_query(db, B)
		return 'select ' .. nl_(db) .. ' as DBN, s.name collate database_default as SCH, o.name collate database_default as TN, o.object_id as OID, ' ..
		'cast(t.is_memory_optimized as int) as MO, cast(t.durability as int) as DUR, cast(t.temporal_type as int) as TT, ' ..
		'(select hs.name + ' .. sl('.') .. ' + ht.name from ' .. B .. '.sys.tables ht join ' .. B .. '.sys.schemas hs on hs.schema_id = ht.schema_id where ht.history_table_id = t.object_id) collate database_default as HPARENT, ' ..
		(LEDGER and 'cast(t.ledger_type as int)' or 'cast(0 as int)') .. ' as LT, ' ..
		(GRAPH and '(cast(t.is_node as int) + 2 * cast(t.is_edge as int))' or 'cast(0 as int)') .. ' as GR, ' ..
		'(select sum(p.rows) from ' .. B .. '.sys.partitions p where p.object_id = o.object_id and p.index_id in (0, 1)) as RWS, ' ..
		'cast(i.type as int) as ITYPE, cast(i.is_disabled as int) as IDIS, ' ..
		'(select count(*) from ' .. B .. '.sys.security_predicates sp join ' .. B .. '.sys.security_policies pol on pol.object_id = sp.object_id where pol.is_enabled = 1 and sp.predicate_type = 0 and sp.target_object_id = o.object_id) as RLS, ' ..
		'(select count(*) from ' .. B .. '.sys.masked_columns mc where mc.object_id = o.object_id and mc.is_masked = 1) as MSK, ' ..
		'pf.name collate database_default as PF, pc.name collate database_default as PCOL, ' ..
		'kc.name collate database_default as KCOL, type_name(kc.system_type_id) as KTYPE, cast(kc.max_length as int) as KLEN, cast(kc.precision as int) as KPREC, cast(kc.scale as int) as KSCALE, ' ..
		'cast(kc.is_nullable as int) as KNULL, kc.collation_name collate database_default as KCOLL, cast(case when kc.system_type_id = kc.user_type_id then 0 else 1 end as int) as KALIAS ' ..
		'from ' .. B .. '.sys.tables t join ' .. objfrom(B) .. ' on o.object_id = t.object_id ' ..
		'join ' .. B .. '.sys.indexes i on i.object_id = o.object_id and i.index_id <= 1 ' ..
		'left join ' .. B .. '.sys.partition_schemes ps on ps.data_space_id = i.data_space_id ' ..
		'left join ' .. B .. '.sys.partition_functions pf on pf.function_id = ps.function_id ' ..
		'left join ' .. B .. '.sys.index_columns pic on pic.object_id = o.object_id and pic.index_id = i.index_id and pic.partition_ordinal = 1 ' ..
		'left join ' .. B .. '.sys.columns pc on pc.object_id = o.object_id and pc.column_id = pic.column_id ' ..
		'left join ' .. B .. '.sys.index_columns ic on ic.object_id = o.object_id and ic.index_id = 1 and ic.key_ordinal = 1 and i.type = 1 ' ..
		'left join ' .. B .. '.sys.columns kc on kc.object_id = o.object_id and kc.column_id = ic.column_id ' ..
		'where t.is_external = 0 and ' .. (LEDGER and ('t.is_dropped_ledger_table = 0 and not exists (select 1 from ' .. B .. '.sys.tables dl where dl.history_table_id = t.object_id and dl.is_dropped_ledger_table = 1) and ') or '') .. OBJW
	end
	local ok_tr, tr = pcall(remote_all, rel_query, UPPER4)
	if not ok_tr then
		local err, keep = tr, {}
		for _, d in ipairs(DBS) do
			if premote('select top 0 1 as X from ' .. qb(d.name) .. '.sys.objects') then keep[#keep + 1] = d
			else note('SKIPPED DATABASE', 'database ' .. oneline(d.name) .. ' is not migrated: the connection user may not read its catalog (SELECT denied, e.g. db_denydatareader)', true) end
		end
		if #keep == #DBS then error(err, 0) end                      -- no database is unreadable: the original error stands
		DBS = keep
		tr = remote_all(rel_query, UPPER4)
	end
	do
		-- SQL Server checks the catalog SELECT permission in the context of the first database of a statement only, so a
		-- database whose catalog is not readable can also come back silently empty: probe every database without a match
		local seen, keep = {}, {}
		for i = 1, #tr do seen[tr[i][1]] = true end
		for _, d in ipairs(DBS) do
			if seen[d.name] or premote('select top 0 1 as X from ' .. qb(d.name) .. '.sys.objects') then keep[#keep + 1] = d
			else note('SKIPPED DATABASE', 'database ' .. oneline(d.name) .. ' is not migrated: the connection user may not read its catalog (SELECT denied, e.g. db_denydatareader)', true) end
		end
		DBS = keep
	end
	vd_notes()
	for i = 1, #tr do
		local x = tr[i]
		local r = new_rel(x, 'U')
		r.mo, r.dur, r.tt, r.hparent, r.lt, r.gr = int(x[5]) == 1, int(x[6]), int(x[7]), str(x[8]), int(x[9]), int(x[10])
		r.rows = tonumber(x[11]) or 0
		r.itype, r.idis, r.rls, r.msk = int(x[12]), int(x[13]) == 1, int(x[14]), int(x[15])
		r.pf, r.pcol = str(x[16]), str(x[17])
		r.key = (not isnull(x[18])) and {col = x[18], typ = str(x[19]), len = int(x[20]), prec = int(x[21]), scale = int(x[22]),
		                                  null_ = int(x[23]) == 1, coll = str(x[24]), alias = int(x[25]) == 1} or nil
	end
end

-- views (review section) and indexed views
local VIEWS = {}
if G_VIEWS or G_IV then
	local vr = remote_all(function(db, B)
		return 'select ' .. nl_(db) .. ' as DBN, s.name collate database_default as SCH, o.name collate database_default as TN, o.object_id as OID, ' ..
		'cast(case when exists (select 1 from ' .. B .. '.sys.indexes i where i.object_id = o.object_id and i.index_id = 1) then 1 else 0 end as int) as IDX, ' ..
		'(select sum(p.rows) from ' .. B .. '.sys.partitions p where p.object_id = o.object_id and p.index_id = 1) as RWS, ' ..
		'cast(case when exists (select 1 from ' .. B .. '.sys.indexes i where i.object_id = o.object_id and i.index_id = 1 and i.is_disabled = 1) then 1 else 0 end as int) as IDIS, ' ..
		'cast(left(m.definition, 1990000) as nvarchar(max)) collate database_default as DEF ' ..
		'from ' .. B .. '.sys.views v join ' .. objfrom(B) .. ' on o.object_id = v.object_id ' ..
		'left join ' .. B .. '.sys.sql_modules m on m.object_id = o.object_id ' ..
		'where ' .. (LEDGER and ('o.object_id not in (select lt.ledger_view_id from ' .. B .. '.sys.tables lt where lt.ledger_view_id is not null) and ') or '') .. OBJW
	end, UPPER4)
	for i = 1, #vr do
		local x = vr[i]
		local v = {db = x[1], sch = x[2], tn = x[3], oid = int(x[4]), idx = int(x[5]) == 1, rows = tonumber(x[6]) or 0, idis = int(x[7]) == 1, def = str(x[8])}
		v.label = oneline(v.db) .. '.' .. oneline(v.sch) .. '.' .. oneline(v.tn)
		if v.idx and G_IV then
			local r = new_rel(x, 'V')
			r.mo, r.tt, r.lt, r.gr, r.rows, r.itype, r.idis, r.rls, r.msk = false, 0, 0, 0, v.rows, 1, v.idis, 0, 0
			v.migrated = r
		end
		VIEWS[#VIEWS + 1] = v
	end
	table.sort(VIEWS, function(a, b) return a.label < b.label end)
end
do
	local has, empty = {}, {}
	for _, r in ipairs(RELS) do has[r.db] = true end
	for _, d in ipairs(DBS) do if not has[d.name] then empty[#empty + 1] = oneline(d.name) end end
	if #empty > 0 and #empty < #DBS then
		note('NO TABLE', 'no table matched SCHEMA_FILTER / TABLE_FILTER in database(s) ' .. table.concat(empty, ', ') ..
		     ' - if tables are expected there, check that the connection user may SELECT them and see their schema (VIEW DEFINITION)', false)
	end
end

table.sort(RELS, function(a, b) if a.exa_schema ~= b.exa_schema then return a.exa_schema < b.exa_schema end
	if a.exa_table ~= b.exa_table then return a.exa_table < b.exa_table end return a.label < b.label end)

-- target names: system schemas, length, collisions
for _, r in ipairs(RELS) do
	local u = r.exa_schema:upper()
	if u == 'SYS' or u == 'EXA_STATISTICS' or u == 'EXA_SYSTEM' then
		fail('target schema ' .. r.exa_schema .. ' collides with an Exasol system schema - set TARGET_SCHEMA')
	end
	if ulen(r.exa_schema) > 128 or ulen(r.exa_table) > 128 then
		r.migrate = false
		note('NAME TOO LONG', 'table ' .. r.label .. ' maps to an Exasol name longer than 128 characters (' .. oneline(r.exa_schema) .. '.' .. oneline(r.exa_table) .. ') and is not migrated', true)
	end
end
do
	local seen = {}
	for _, r in ipairs(RELS) do
		if r.migrate then
			local k = r.exa_schema .. NL .. r.exa_table
			if seen[k] then
				seen[k].migrate = false; r.migrate = false
				note('NAME COLLISION', 'source tables ' .. seen[k].label .. ' and ' .. r.label .. ' map to the same Exasol table ' .. oneline(r.exa_schema) .. '.' .. oneline(r.exa_table) .. ' - neither is migrated; use IDENTIFIER_CASE_INSENSITIVE = false, DB2SCHEMA or separate TARGET_SCHEMA runs', true)
			else
				seen[k] = r
			end
		end
	end
	local sdone = {}
	for _, r in ipairs(RELS) do
		if r.migrate and RESERVED[r.exa_schema:upper()] and not sdone[r.exa_schema] then
			sdone[r.exa_schema] = true
			note('RESERVED NAME', 'the schema name ' .. oneline(r.exa_schema) .. ' is a reserved word in Exasol and must be quoted in queries', false)
		end
	end
end

-- notes about table kinds
for _, r in ipairs(RELS) do
	if r.migrate then
		if r.rls and r.rls > 0 then note('ROW LEVEL SECURITY', 'table ' .. r.label .. ' has an enabled row-level security filter predicate - the IMPORT reads only the rows the connection user may see (this can apply to sysadmin logins too); migrate with a login that the predicate exempts. The CHECK metric ROW_CNT_CATALOG shows a difference', true) end
		if r.msk and r.msk > 0 then note('MASKED COLUMNS', 'table ' .. r.label .. ' has ' .. istr(r.msk) .. ' dynamically masked column(s) - a connection user without UNMASK permission migrates the masked values, and CHECK_MIGRATION then reports DEVIATIONs for them (SQL Server masks the source aggregates as well)', true) end
		if r.gr == 1 or r.gr == 2 then note('GRAPH TABLE', 'graph ' .. (r.gr == 1 and 'node' or 'edge') .. ' table ' .. r.label .. ' is migrated as a plain table: internal graph columns are skipped, the pseudo columns become NODE_ID / EDGE_ID / FROM_ID / TO_ID (JSON text with SQL Server object ids); MATCH queries are not migrated', false) end
		if r.mo then note('MEMORY-OPTIMIZED', 'table ' .. r.label .. ' is memory-optimized and is read with WITH (SNAPSHOT)' .. (r.dur == 1 and ' - it is SCHEMA_ONLY (non-durable): its rows are lost at every SQL Server restart' or ''), false) end
		if r.tt == 1 then note('HISTORY TABLE', 'table ' .. r.label .. ' is the system-versioning history table of ' .. oneline(r.hparent or 'unknown') .. '; it is migrated as a plain table (period columns are UTC)' .. ((r.tn:find('^MSSQL_TemporalHistoryFor_') or r.tn:find('^MSSQL_LedgerHistoryFor_')) and ' - its name contains the SQL Server object id: after the source table is re-created a re-run creates a NEW history table, drop the old one manually' or ''), false) end
		if r.tt == 2 then note('HISTORY TABLE', 'table ' .. r.label .. ' is system-versioned; its period columns (UTC) are migrated as plain columns, the history table only when it matches the filters', false) end
		if r.lt == 1 then note('LEDGER TABLE', 'table ' .. r.label .. ' is a ledger history table; it is migrated as a plain table' .. (r.tn:find('^MSSQL_LedgerHistoryFor_') and ' - its name contains the SQL Server object id: after the source table is re-created a re-run creates a NEW history table, drop the old one manually' or ''), false)
		elseif r.lt == 2 or r.lt == 3 then note('LEDGER TABLE', 'ledger table ' .. r.label .. ' is migrated as a plain table including its hidden ledger columns; ledger verification is not migrated', false) end
		if r.idis then
			r.noimport = true
			note('DISABLED INDEX', 'table ' .. r.label .. ' has a disabled clustered index (or columnstore index) and cannot be read in SQL Server - it is created empty, without IMPORT; rebuild the index and generate again', true)
		end
		if r.kind == 'V' then note('INDEXED VIEW', 'indexed view ' .. r.label .. ' is migrated as a table with its stored rows (read WITH (NOEXPAND)); Exasol does not maintain it', false) end
	end
end
for _, v in ipairs(VIEWS) do
	if v.def == nil then note('ENCRYPTED VIEW', 'the definition of view ' .. v.label .. ' is not available (WITH ENCRYPTION, or no VIEW DEFINITION permission) - it is listed without its T-SQL', false) end
	if v.idx and not G_IV then note('INDEXED VIEW', 'indexed view ' .. v.label .. ' is not migrated (set MIGRATE_INDEXED_VIEWS = true to migrate its stored rows as a table)', false) end
end

-------------------------------------------------------------------------------------------------------------
-- columns
-------------------------------------------------------------------------------------------------------------
do
	local types_w = G_IV and "('U', 'V')" or "('U')"
	local cr = remote_all(function(db, B)
		return 'select ' .. nl_(db) .. ' as DBN, c.object_id as OID, c.column_id as CID, c.name collate database_default as CN, ' ..
		'case when ty.is_user_defined = 0 then ty.name else type_name(ty.system_type_id) end collate database_default as BT, ty.name collate database_default as TYN, ' ..
		'cast(ty.is_assembly_type as int) as CLR, cast(ty.is_user_defined as int) as UD, cast(c.max_length as int) as ML, ' ..
		'cast(c.precision as int) as PR, cast(c.scale as int) as SC, cast(c.is_nullable as int) as NUL, cast(c.is_identity as int) as IDN, ' ..
		'cast(case when c.is_computed = 1 and not exists (select 1 from ' .. B .. '.sys.computed_columns cc where cc.object_id = c.object_id and cc.column_id = c.column_id and cc.is_persisted = 1) then 2 else c.is_computed end as int) as CMP, cast(c.is_column_set as int) as CSET, ' .. (GRAPH and 'cast(c.graph_type as int)' or 'cast(null as int)') .. ' as GT, ' ..
		'cast(collationproperty(c.collation_name, ' .. sl('CodePage') .. ') as int) as CP, dc.definition collate database_default as DDEF, ' ..
		'cast(case when c.default_object_id <> 0 and dc.object_id is null then 1 else 0 end as int) as BOUND, ' ..
		'cast(has_perms_by_name(quotename(' .. nl_(db) .. ') + ' .. sl('.') .. ' + quotename(s.name) + ' .. sl('.') .. ' + quotename(o.name), ' .. sl('OBJECT') .. ', ' .. sl('SELECT') .. ', c.name, ' .. sl('COLUMN') .. ') as int) as SELP, ' ..
		'cast(case when c.encryption_type is null then 0 else 1 end as int) as ENC ' ..
		'from ' .. B .. '.sys.columns c join ' .. objfrom(B) .. ' on o.object_id = c.object_id ' ..
		'join ' .. B .. '.sys.types ty on ty.user_type_id = c.user_type_id ' ..
		'left join ' .. B .. '.sys.default_constraints dc on dc.object_id = c.default_object_id ' ..
		'where o.type in ' .. types_w .. ' and ' .. OBJW
	end, 't.*, upper(t."CN")')
	for i = 1, #cr do
		local x = cr[i]
		local r = REL_BY[rkey(x[1], int(x[2]))]
		if r then
			r.cols[#r.cols + 1] = {num = int(x[3]), name = x[4], bt = str(x[5]), tyn = str(x[6]), clr = int(x[7]) == 1, ud = int(x[8]) == 1,
			                       ml = int(x[9]), pr = int(x[10]), sc = int(x[11]), nullable = int(x[12]) == 1, identity = int(x[13]) == 1,
			                       computed = int(x[14]) >= 1, volatile = int(x[14]) == 2, colset = int(x[15]) == 1, gt = int(x[16]), cp = int(x[17]),
			                       def = str(x[18]), bound = int(x[19]) == 1, selp = int(x[20]), enc = int(x[21]) == 1, name_up = x[22]}
		end
	end
end

-------------------------------------------------------------------------------------------------------------
-- type mapping: Exasol type + T-SQL source expression per column
-------------------------------------------------------------------------------------------------------------
local KNOWN = {}
for _, t in ipairs({'bit', 'tinyint', 'smallint', 'int', 'bigint', 'decimal', 'numeric', 'money', 'smallmoney', 'float', 'real',
	'char', 'varchar', 'text', 'nchar', 'nvarchar', 'ntext', 'sysname', 'uniqueidentifier', 'xml', 'json', 'vector',
	'date', 'datetime', 'smalldatetime', 'datetime2', 'time', 'datetimeoffset', 'binary', 'varbinary', 'image',
	'timestamp', 'rowversion', 'sql_variant', 'hierarchyid', 'geometry', 'geography'}) do KNOWN[t] = true end
local INTS = {tinyint = 3, smallint = 5, int = 10, bigint = 19}
local SC_COLL = ' collate Latin1_General_100_CI_AS_SC'
local function long_text(c)
	-- (n)varchar(max)/text/ntext/xml/json/vector: optional cut at 2,000,000 characters (supplementary characters count once)
	if TRUNC then return 'left(cast(' .. c .. ' as nvarchar(max))' .. SC_COLL .. ', 2000000)' end
	return 'cast(' .. c .. ' as nvarchar(max))'
end
local function hexsrc(c) return 'convert(varchar(max), cast(' .. c .. ' as varbinary(max)), 2)' end
-- an invalid geometry that contains curves: Exasol GEOMETRY has no curves, so it is made valid and then linearised
local function geo_curved(c)
	local w = 'cast(' .. c .. '.STAsText() as nvarchar(max)) collate Latin1_General_BIN2'
	return '(' .. w .. ' like N' .. sl('%CURVE%') .. ' or ' .. w .. ' like N' .. sl('%CIRCULARSTRING%') .. ')'
end

local function oor_dto(c, n, name)
	-- c: datetimeoffset(n) column; returns its UTC value as datetime2(n) (NULL passes) and applies TEMPORAL_OUT_OF_RANGE to
	-- values outside 0001-01-02 .. 9999-12-30 UTC (values on the outermost day cannot be displayed in every session time
	-- zone). The range test compares the datetimeoffset itself (it compares by its UTC instant); CONVERT style 1 converts
	-- to UTC once and costs a fraction of AT TIME ZONE.
	local ty, dt = 'datetime2(' .. istr(n) .. ')', 'datetimeoffset(' .. istr(n) .. ')'
	local frac = (n > 0) and ('.' .. string.rep('9', n)) or ''
	local e = 'convert(' .. ty .. ', ' .. c .. ', 1)'
	local lo = 'cast(' .. sl('0001-01-02 00:00:00 +00:00') .. ' as ' .. dt .. ')'
	local hi = 'cast(' .. sl('9999-12-30 23:59:59' .. frac .. ' +00:00') .. ' as ' .. dt .. ')'
	if OORMODE == 'NULL' then
		return 'case when ' .. c .. ' >= ' .. lo .. ' and ' .. c .. ' <= ' .. hi .. ' then ' .. e .. ' end'
	elseif OORMODE == 'CLAMP' then
		return 'case when ' .. c .. ' < ' .. lo .. ' then cast(' .. sl('0001-01-02 00:00:00') .. ' as ' .. ty .. ') when ' .. c .. ' > ' .. hi ..
		       ' then cast(' .. sl('9999-12-30 23:59:59' .. frac) .. ' as ' .. ty .. ') else ' .. e .. ' end'
	end
	-- FAIL: the source query fails with a message that names the column and the value (int conversion of a text with both)
	return 'case when ' .. c .. ' is null or (' .. c .. ' >= ' .. lo .. ' and ' .. c .. ' <= ' .. hi .. ') then ' .. e ..
	       ' else dateadd(day, cast(N' .. sl('TEMPORAL_OUT_OF_RANGE column ') .. ' + ' .. nl_(usub(oneline(name), 128)) .. ' + N' .. sl(' value ') ..
	       ' + convert(nvarchar(34), ' .. c .. ', 121) as int), cast(null as ' .. ty .. ')) end'
end
local function map_col(col)
	-- returns {typ, src, cls, ...}; cls: dec, dbl, date, ts, tsz, char, text, time, uuid, hash, hex, geo, numtext
	local c = qb(col.name)
	local t = col.bt
	local m = {cls = 'text'}
	if (col.ud and col.clr) or t == nil or not KNOWN[t] then m.unsupported = true return m end
	local utf8 = (col.cp == 65001)
	if t == 'bit' then
		m.typ = 'DECIMAL(1,0)'; m.cls = 'dec'; m.p = 1; m.s = 0; m.exact = true; m.distinct = true; m.src = c
		m.chk = 'cast(' .. c .. ' as tinyint)'; m.bit = true
	elseif INTS[t] then
		m.typ = 'DECIMAL(' .. istr(INTS[t]) .. ',0)'; m.cls = 'dec'; m.p = INTS[t]; m.s = 0; m.exact = true; m.distinct = true; m.src = c
	elseif t == 'decimal' or t == 'numeric' then
		local p, s = col.pr, col.sc
		if p <= 36 then
			m.typ = 'DECIMAL(' .. istr(p) .. ',' .. istr(s) .. ')'; m.cls = 'dec'; m.p = p; m.s = s; m.exact = true; m.distinct = true; m.src = c
		elseif DECOF == 'DOUBLE' then
			m.typ = 'DOUBLE'; m.cls = 'dbl'; m.src = 'cast(' .. c .. ' as float)'
		elseif DECOF == 'VARCHAR' then
			m.typ = 'VARCHAR(41) ASCII'; m.cls = 'numtext'; m.src = 'convert(varchar(41), ' .. c .. ')'
		else
			-- keep the scale (decimal(38,2) -> DECIMAL(36,2), as common in SQL Server); a scale above 35 is rounded to 35 so that
			-- at least one integer digit remains; values with more integer digits than 36 - scale make the IMPORT fail loudly
			local cs = math.min(s, 35)
			m.typ = 'DECIMAL(36,' .. istr(cs) .. ')'; m.cls = 'dec'; m.p = 36; m.s = cs; m.exact = true; m.narrowed = p - s > 36 - cs
			if cs < s then m.capped = true; m.src = 'cast(' .. c .. ' as decimal(38,' .. istr(cs) .. '))' else m.src = c; m.distinct = true end
		end
	elseif t == 'money' or t == 'smallmoney' then
		m.typ = (t == 'money') and 'DECIMAL(19,4)' or 'DECIMAL(10,4)'; m.cls = 'dec'; m.p = (t == 'money') and 19 or 10; m.s = 4
		m.exact = true; m.distinct = true; m.src = c
	elseif t == 'float' or t == 'real' then
		m.typ = 'DOUBLE'; m.cls = 'dbl'; m.src = c
	elseif t == 'char' then
		local n = col.ml
		m.typ = (n <= 2000) and ('CHAR(' .. istr(n) .. ') UTF8') or ('VARCHAR(' .. istr(n) .. ') UTF8'); m.cls = (n <= 2000) and 'char' or 'text'
		m.src = utf8 and c or ((n <= 4000) and ('cast(' .. c .. ' as nchar(' .. istr(n) .. '))') or ('cast(' .. c .. ' as nvarchar(max))'))
		m.plain_text = (n > 2000)
	elseif t == 'varchar' then
		if col.ml < 0 then
			m.typ = 'VARCHAR(2000000) UTF8'; m.src = (utf8 and not TRUNC) and c or long_text(c)
		else
			m.typ = 'VARCHAR(' .. istr(col.ml) .. ') UTF8'
			m.src = utf8 and c or ((col.ml <= 4000) and ('cast(' .. c .. ' as nvarchar(' .. istr(col.ml) .. '))') or ('cast(' .. c .. ' as nvarchar(max))'))
		end
		m.plain_text = true
	elseif t == 'text' or t == 'ntext' then
		m.typ = 'VARCHAR(2000000) UTF8'; m.src = long_text(c); m.plain_text = true
	elseif t == 'nchar' then
		local n = math.floor(col.ml / 2)
		m.typ = (n <= 2000) and ('CHAR(' .. istr(n) .. ') UTF8') or ('VARCHAR(' .. istr(n) .. ') UTF8'); m.cls = (n <= 2000) and 'char' or 'text'
		m.src = c; m.plain_text = (n > 2000)
	elseif t == 'nvarchar' then
		if col.ml < 0 then m.typ = 'VARCHAR(2000000) UTF8'; m.src = TRUNC and long_text(c) or c
		else m.typ = 'VARCHAR(' .. istr(math.floor(col.ml / 2)) .. ') UTF8'; m.src = c end
		m.plain_text = true
	elseif t == 'sysname' then
		m.typ = 'VARCHAR(128) UTF8'; m.src = c; m.plain_text = true
	elseif t == 'uniqueidentifier' then
		m.typ = 'CHAR(36) ASCII'; m.cls = 'uuid'; m.distinct = true; m.src = c
	elseif t == 'xml' or t == 'json' or t == 'vector' then
		m.typ = 'VARCHAR(2000000) UTF8'; m.src = long_text(c); m.structured = true
	elseif t == 'sql_variant' then
		local bt = 'cast(sql_variant_property(' .. c .. ', ' .. sl('BaseType') .. ') as varchar(30)) collate Latin1_General_BIN2'
		m.typ = 'VARCHAR(16100) UTF8'; m.plain_text = true
		m.src = 'case ' .. bt ..
			' when ' .. sl('float') .. ' then convert(nvarchar(30), cast(' .. c .. ' as float), 3)' ..
			' when ' .. sl('real') .. ' then convert(nvarchar(30), cast(' .. c .. ' as real), 3)' ..
			' when ' .. sl('datetime') .. ' then convert(nvarchar(23), cast(' .. c .. ' as datetime), 121)' ..
			' when ' .. sl('smalldatetime') .. ' then convert(nvarchar(19), cast(' .. c .. ' as smalldatetime), 120)' ..
			' when ' .. sl('datetime2') .. ' then convert(nvarchar(27), cast(' .. c .. ' as datetime2(7)), 121)' ..
			' when ' .. sl('date') .. ' then convert(nvarchar(10), cast(' .. c .. ' as date), 23)' ..
			' when ' .. sl('time') .. ' then cast(cast(' .. c .. ' as time(7)) as nvarchar(16))' ..
			' when ' .. sl('datetimeoffset') .. ' then convert(nvarchar(34), cast(' .. c .. ' as datetimeoffset(7)), 121)' ..
			' when ' .. sl('money') .. ' then convert(nvarchar(30), cast(' .. c .. ' as money), 2)' ..
			' when ' .. sl('smallmoney') .. ' then convert(nvarchar(30), cast(' .. c .. ' as smallmoney), 2)' ..
			' when ' .. sl('binary') .. ' then convert(varchar(max), cast(' .. c .. ' as varbinary(8000)), 2)' ..
			' when ' .. sl('varbinary') .. ' then convert(varchar(max), cast(' .. c .. ' as varbinary(8000)), 2)' ..
			' when ' .. sl('varchar') .. ' then cast(cast(' .. c .. ' as varchar(8000)) as nvarchar(max))' ..
			' when ' .. sl('char') .. ' then cast(cast(' .. c .. ' as varchar(8000)) as nvarchar(max))' ..
			' else cast(' .. c .. ' as nvarchar(4000)) end'
	elseif t == 'date' then
		m.typ = 'DATE'; m.cls = 'date'; m.distinct = true; m.src = c
	elseif t == 'datetime' then
		m.typ = 'TIMESTAMP(3)'; m.cls = 'ts'; m.tsp = 3; m.distinct = true; m.src = c
	elseif t == 'smalldatetime' then
		m.typ = 'TIMESTAMP(0)'; m.cls = 'ts'; m.tsp = 0; m.distinct = true; m.src = c; m.small = true
	elseif t == 'datetime2' then
		m.typ = 'TIMESTAMP(' .. istr(col.sc) .. ')'; m.cls = 'ts'; m.tsp = col.sc; m.distinct = true; m.src = c
	elseif t == 'datetimeoffset' then
		m.typ = 'TIMESTAMP(' .. istr(col.sc) .. ') WITH LOCAL TIME ZONE'; m.cls = 'tsz'; m.tsp = col.sc
		m.src = oor_dto(c, col.sc, col.name)
	elseif t == 'time' then
		m.typ = 'VARCHAR(16) ASCII'; m.cls = 'time'; m.tsp = col.sc; m.src = 'cast(' .. c .. ' as varchar(16))'
	elseif t == 'binary' or t == 'varbinary' or t == 'image' or t == 'timestamp' or t == 'rowversion' then
		if BINMODE == 'SKIP' then m.skipped = true; m.typ = '-'; m.src = 'null'; return m end
		local n = col.ml
		if t == 'timestamp' or t == 'rowversion' then
			m.src = 'convert(varchar(16), cast(' .. c .. ' as varbinary(8)), 2)'
			if BINMODE == 'HASHTYPE' then m.typ = 'HASHTYPE(8 BYTE)'; m.cls = 'hash' else m.typ = 'VARCHAR(16) ASCII'; m.cls = 'hex' end
		elseif t == 'binary' and BINMODE == 'HASHTYPE' and n <= 1024 then
			m.typ = 'HASHTYPE(' .. istr(n) .. ' BYTE)'; m.cls = 'hash'; m.src = hexsrc(c)
		else
			local len = (t == 'image' or n < 0 or 2 * n > 2000000) and 2000000 or 2 * n
			m.typ = 'VARCHAR(' .. istr(len) .. ') ASCII'; m.cls = 'hex'; m.src = hexsrc(c)
		end
	elseif t == 'hierarchyid' then
		m.typ = 'VARCHAR(4000) UTF8'; m.src = c .. '.ToString()'
	elseif t == 'geometry' then
		-- curves become line approximations; STCurveToLine fails on invalid geometries (error 24144) and turns POINT EMPTY
		-- into GEOMETRYCOLLECTION EMPTY - those keep their plain WKT
		m.typ = 'GEOMETRY'; m.cls = 'geo'
		m.src = 'case when ' .. c .. '.STIsValid() = 1 and ' .. c .. '.STIsEmpty() = 0 then ' .. c .. '.STCurveToLine().STAsText() when ' .. geo_curved(c) ..
		        ' then ' .. c .. '.MakeValid().STCurveToLine().STAsText() else ' .. c .. '.STAsText() end'
	elseif t == 'geography' then
		m.typ = 'GEOMETRY'; m.cls = 'geo'
		m.src = 'case when cast(' .. c .. '.STGeometryType() as nvarchar(30)) collate Latin1_General_BIN2 = ' .. sl('FullGlobe') .. ' then null when ' ..
		        c .. '.STIsValid() = 1 and ' .. c .. '.STIsEmpty() = 0 then ' .. c .. '.STCurveToLine().STAsText() when ' .. geo_curved(c) ..
		        ' then ' .. c .. '.MakeValid().STCurveToLine().STAsText() else ' .. c .. '.STAsText() end'
	else
		m.unsupported = true
	end
	return m
end

-------------------------------------------------------------------------------------------------------------
-- DEFAULT mapping (validated per mapped type; returns ' DEFAULT ...', or nil + reason)
-------------------------------------------------------------------------------------------------------------
local QM = string.char(63)       -- question mark (kept out of the script text: DbVisualizer parameter marker)
local COLON = string.char(58)    -- colon (a colon before a quote is a DbVisualizer parameter marker too)
local function unwrap(d)
	d = trim(d)
	while d:sub(1, 1) == '(' and d:sub(-1) == ')' do
		local depth, ok, inq = 0, true, false
		for i = 1, #d do
			local ch = d:sub(i, i)
			if ch == SQ then inq = not inq
			elseif not inq then
				if ch == '(' then depth = depth + 1
				elseif ch == ')' then depth = depth - 1; if depth == 0 and i < #d then ok = false break end end
			end
		end
		if not ok then break end
		d = trim(d:sub(2, -2))
	end
	local inner = d:match('^%-%((.*)%)$')
	if inner then return '-' .. unwrap(inner) end
	return d
end
local function civil(days)   -- days since 1970-01-01 -> year, month, day (proleptic Gregorian)
	local z = days + 719468
	local era = (z >= 0 and z or z - 146096) // 146097
	local doe = z - era * 146097
	local yoe = (doe - doe // 1460 + doe // 36524 - doe // 146096) // 365
	local y = yoe + era * 400
	local doy = doe - (365 * yoe + yoe // 4 - yoe // 100)
	local mp = (5 * doy + 2) // 153
	local d = doy - (153 * mp + 2) // 5 + 1
	local mo = (mp < 10) and (mp + 3) or (mp - 9)
	if mo <= 2 then y = y + 1 end
	return y, mo, d
end
local function strlit(v)
	local s = v:match('^N' .. SQ .. '(.*)' .. SQ .. '$') or v:match('^' .. SQ .. '(.*)' .. SQ .. '$')
	if s == nil or (s:gsub(SQ .. SQ, '')):find(SQ) then return nil end
	return (s:gsub(SQ .. SQ, SQ))
end
local function numparts(v)
	-- decimal literal -> sign, integer digits, fraction digits (nil if not a plain decimal literal)
	v = v:gsub('^%$', '')
	local sg, ip, fp = v:match('^([%+%-]' .. QM .. ')(%d*)%.' .. QM .. '(%d*)$')
	if sg == nil or (ip == '' and fp == '') then return nil end
	if not v:find('^[%+%-]' .. QM .. '%d*%.' .. QM .. '%d*$') then return nil end
	return (sg == '-') and '-' or '', ip, fp
end
local function round_dec(sg, ip, fp, s)
	-- round half away from zero to s fractional digits (string arithmetic); returns sign, int digits, frac digits
	ip = (ip == '') and '0' or ip
	if #fp <= s then return sg, ip, fp .. string.rep('0', s - #fp) end
	local keep, nxt = fp:sub(1, s), tonumber(fp:sub(s + 1, s + 1))
	local digits = ip .. keep
	if nxt >= 5 then
		local t, carry = {}, 1
		for i = #digits, 1, -1 do
			local dd = tonumber(digits:sub(i, i)) + carry
			carry = (dd >= 10) and 1 or 0
			t[i] = istr(dd % 10)
		end
		digits = (carry == 1 and '1' or '') .. table.concat(t)
	end
	return sg, digits:sub(1, #digits - s), digits:sub(#digits - s + 1)
end
local NOW = {['GETDATE()'] = 'L', ['SYSDATETIME()'] = 'L', ['CURRENT_TIMESTAMP'] = 'L', ['GETUTCDATE()'] = 'U', ['SYSUTCDATETIME()'] = 'U', ['SYSDATETIMEOFFSET()'] = 'O'}
local function parse_ts(s)
	local y, mo, d, rest = s:match('^(%d%d%d%d)%-(%d%d)%-(%d%d)(.*)$')
	if not y then y, mo, d, rest = s:match('^(%d%d%d%d)(%d%d)(%d%d)(.*)$') end
	if not y or tonumber(mo) < 1 or tonumber(mo) > 12 or tonumber(d) < 1 then return nil end
	local yy, mm = tonumber(y), tonumber(mo)
	local mdays = ({31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31})[mm]
	if mm == 2 and ((yy % 4 == 0 and yy % 100 ~= 0) or yy % 400 == 0) then mdays = 29 end
	if tonumber(d) > mdays then return nil end
	local hh, mi, ss, fr = '00', '00', '00', ''
	if rest ~= '' then
		local h2, m2, r2 = rest:match('^[T ](%d%d)' .. COLON .. '(%d%d)(.*)$')
		if not h2 then return nil end
		hh, mi = h2, m2
		if r2 ~= '' then
			local s2, f2 = r2:match('^' .. COLON .. '(%d%d)%.' .. QM .. '(%d*)$')
			if not s2 then return nil end
			ss, fr = s2, f2
		end
	end
	return y .. '-' .. mo .. '-' .. d, hh .. COLON .. mi .. COLON .. ss, fr
end

local function map_default(col, m)
	if col.identity or col.computed then return '' end
	if col.bound then return nil, '(bound default object, sp_bindefault)' end
	local d = col.def
	if d == nil or d == '' then return '' end
	if d:find(string.char(0), 1, true) then return nil, d end
	local v = unwrap(d)
	if v:upper() == 'NULL' then return '' end
	local fn = NOW[v:upper():gsub('%s', '')]
	local s = strlit(v)
	local cls = m.cls
	if cls == 'dec' then
		local lit = s or v
		local sg, ip, fp = numparts(lit)
		if not sg and m.bit and s and (s:upper() == 'TRUE' or s:upper() == 'FALSE') then return (s:upper() == 'TRUE') and ' DEFAULT 1' or ' DEFAULT 0' end
		if not sg then
			local e = tonumber(lit)
			if e and lit:find('[eE]') and math.abs(e) < 2^53 then
				e = (e < 0) and -math.floor(-e) or math.floor(e)
				if m.s == 0 then sg, ip, fp = (e < 0) and '-' or '', istr(math.abs(e)), '' end
			end
		end
		if not sg then return nil, d end
		if m.bit then
			local nz = (ip .. fp):find('[1-9]') ~= nil
			return nz and ' DEFAULT 1' or ' DEFAULT 0'
		end
		if INTS[col.bt] then fp = '' end                           -- SQL Server truncates a numeric literal for integer columns (decimal rounds)
		local rs, ri, rf = round_dec(sg, ip, fp, m.s)
		ri = ri:gsub('^0+(%d)', '%1')
		if #ri:gsub('^0+', '') > m.p - m.s then return nil, d end
		local IRANGE = {tinyint = {0, 255}, smallint = {-32768, 32767}, int = {-2147483648, 2147483647}, bigint = {-9.2233720368547758e18, 9.2233720368547758e18}}
		if IRANGE[col.bt] then
			local nv = tonumber(((rs == '-') and '-' or '') .. ri)
			if nv == nil or nv < IRANGE[col.bt][1] or nv > IRANGE[col.bt][2] then return nil, d end
		end
		return ' DEFAULT ' .. ((ri .. rf):find('[1-9]') and rs or '') .. ri .. ((m.s > 0) and ('.' .. rf) or '')
	elseif cls == 'dbl' then
		local lit = s or v
		if tonumber(lit) and lit:find('^[%+%-]' .. QM .. '[%d%.]+[eE]' .. QM .. '[%+%-]' .. QM .. '%d*$') then return ' DEFAULT ' .. lit end
		return nil, d
	elseif cls == 'date' then
		if fn == 'L' then return ' DEFAULT CURRENT_DATE' end
		if s then local dd = parse_ts(s); if dd then return ' DEFAULT DATE ' .. sl(dd) end end
		return nil, d
	elseif cls == 'ts' then
		if fn == 'L' or fn == 'O' then return ' DEFAULT CURRENT_TIMESTAMP' end
		if fn == 'U' then return ' DEFAULT CONVERT_TZ(CURRENT_TIMESTAMP, SESSIONTIMEZONE, ' .. sl('UTC') .. ')' end
		if s then
			local dd, tt, fr = parse_ts(s)
			if dd then
				if m.tsp and m.tsp > 0 and fr ~= '' then tt = tt .. '.' .. fr:sub(1, m.tsp) end
				return ' DEFAULT TIMESTAMP ' .. sl(dd .. ' ' .. tt)
			end
		elseif col.bt == 'datetime' or col.bt == 'smalldatetime' then
			local n = v:match('^%-' .. QM .. '%d+$') and tonumber(v)
			if n and math.abs(n) < 3000000 then
				local y, mo, dd = civil(-25567 + n)                     -- datetime 0 = 1900-01-01
				if y >= 1753 and y <= 9999 then return ' DEFAULT TIMESTAMP ' .. sl(string.format('%04d-%02d-%02d 00:00:00', y, mo, dd)) end
			end
		end
		return nil, d
	elseif cls == 'tsz' then
		if fn then return ' DEFAULT CURRENT_TIMESTAMP' end
		return nil, d
	elseif cls == 'time' then
		if s then
			local hh, mi, rest = s:match('^(%d%d' .. QM .. ')' .. COLON .. '(%d%d)(.*)$')
			if hh then
				local ss, fr = '00', ''
				if rest ~= '' then ss, fr = rest:match('^' .. COLON .. '(%d%d)%.' .. QM .. '(%d*)$') end
				if ss and tonumber(hh) < 24 and tonumber(mi) < 60 and tonumber(ss) < 60 then
					local p = m.tsp or 7
					local txt = string.format('%02d', tonumber(hh)) .. COLON .. mi .. COLON .. ss
					if p > 0 then txt = txt .. '.' .. (fr .. string.rep('0', p)):sub(1, p) end
					return ' DEFAULT ' .. sl(txt)
				end
			end
		end
		return nil, d
	elseif cls == 'char' or (cls == 'text' and m.plain_text) then
		local txt = s
		if txt == nil and numparts(v) then txt = v end
		if txt == nil or txt == '' then return nil, d end
		local maxlen = tonumber(m.typ:match('%((%d+)%)'))
		if maxlen and ulen(txt) > maxlen then return nil, d end
		if ulen(sl(txt)) > 2000 then return nil, d end
		return ' DEFAULT ' .. sl(txt)
	end
	return nil, d
end

-- per relation: map columns, skip internal columns, renames, collisions, notes
for _, r in ipairs(RELS) do
	if r.migrate then
		table.sort(r.cols, function(a, b) return a.num < b.num end)
		local used, collide, reserved, allnames, ident, comp, compv, capd = {}, false, {}, {}, {}, {}, {}, {}
		for _, col in ipairs(r.cols) do allnames[exa_id(col.name, col.name_up)] = true end
		for _, col in ipairs(r.cols) do
			if col.gt == 1 or col.gt == 3 or col.gt == 4 or col.gt == 6 or col.gt == 7 then
				col.internal = true                                     -- internal graph columns cannot be selected
			elseif col.enc then
				col.internal = true
				note('ENCRYPTED COLUMN', 'column ' .. r.label .. '.' .. oneline(col.name) .. ' is an Always Encrypted column and is not migrated (its values can only be read with the column master key and columnEncryptionSetting=Enabled - migrate it manually)', true)
				if r.key and r.key.col == col.name then r.key = nil end
			elseif col.colset then
				col.internal = true
				note('COLUMN SET', 'column set ' .. r.label .. '.' .. oneline(col.name) .. ' is not migrated (its sparse columns are migrated individually)', false)
			else
				col.m = map_col(col)
				if col.m.unsupported then
					note('UNSUPPORTED TYPE', 'column ' .. r.label .. '.' .. oneline(col.name) .. ' (SQL Server type ' .. tostring(col.tyn) .. ') is not migrated', true)
				elseif col.m.skipped then
					note('BINARY SKIP', 'column ' .. r.label .. '.' .. oneline(col.name) .. ' (' .. tostring(col.tyn) .. ') is not migrated (BINARY_HANDLING = SKIP)', false)
				else
					local en
					if col.gt == 2 then en = (r.gr == 2) and 'EDGE_ID' or 'NODE_ID'
					elseif col.gt == 5 then en = 'FROM_ID'
					elseif col.gt == 8 then en = 'TO_ID'
					else en = exa_id(col.name, col.name_up) end
					if not ICI and col.gt and col.gt >= 2 then en = en:lower() end
					if FORBIDDEN_COL[en] then
						local nn = en .. '_'
						while used[nn] or allnames[nn] do nn = nn .. '_' end
						note('RENAMED COLUMN', 'column ' .. r.label .. '.' .. oneline(col.name) .. ' is migrated as ' .. oneline(nn) .. ' (' .. en .. ' is not allowed as an Exasol column name)', true)
						en = nn
					end
					if ulen(en) > 128 then
						collide = true
						note('NAME TOO LONG', 'column ' .. r.label .. '.' .. oneline(col.name) .. ' has a name longer than 128 characters - the table is not migrated', true)
					end
					if used[en] then
						collide = true
						note('NAME COLLISION', 'columns of ' .. r.label .. ' map to the same Exasol column name ' .. oneline(en) .. ' - the table is not migrated; use IDENTIFIER_CASE_INSENSITIVE = false', true)
					end
					used[en] = true
					col.exa = en
					if RESERVED[en:upper()] then reserved[#reserved + 1] = oneline(en) end
					if col.m.cls == 'tsz' then r.has_tsz = true end
					if col.identity then ident[#ident + 1] = oneline(col.name) end
					if col.computed then comp[#comp + 1] = oneline(col.name); if col.volatile then compv[#compv + 1] = oneline(col.name) end end
					if col.m.cls == 'geo' then r.has_geo = true end
				end
			end
		end
		local ncols, denied, readable = 0, {}, 0
		for _, col in ipairs(r.cols) do
			if col.exa then ncols = ncols + 1 end
			if col.selp == 0 and not col.internal then denied[#denied + 1] = oneline(col.name) elseif not col.internal then readable = readable + 1 end
		end
		if #denied > 0 and readable == 0 then
			r.migrate = false
			note('NO SELECT', 'table ' .. r.label .. ' is not migrated: the connection user may not SELECT it (no SELECT permission, or a table- or schema-level DENY); grant SELECT or migrate the table manually', true)
		elseif #denied > 0 then
			r.migrate = false
			note('COLUMN DENY', 'table ' .. r.label .. ' is not migrated: the connection user may not SELECT column(s) ' .. table.concat(denied, ', ') .. ' (column-level DENY or missing column GRANT); grant SELECT or migrate the table manually', true)
		end
		if ncols == 0 then
			r.migrate = false
			note('NO COLUMNS', 'table ' .. r.label .. ' has no column that can be migrated and is not migrated', true)
		elseif ncols > 4096 then
			r.migrate = false
			note('UNSUPPORTED TABLE', 'table ' .. r.label .. ' has ' .. istr(ncols) .. ' columns; SQL Server selects at most 4,096 columns per query (Exasol allows 10,000 per table) - the table is not migrated', true)
		end
		if collide then r.migrate = false end
		if r.migrate then
			for _, col in ipairs(r.cols) do if col.exa and col.m and col.m.narrowed then capd[#capd + 1] = oneline(col.name) .. ' ' .. col.m.typ end end
			if #capd > 0 then note('DECIMAL CAP', 'table ' .. r.label .. ': column(s) ' .. table.concat(capd, ', ') .. ' hold fewer integer digits than in SQL Server (DECIMAL_OVERFLOW = CAP) - a larger value makes the IMPORT fail; use DOUBLE or VARCHAR if such values exist', false) end
			if #ident > 0 then note('IDENTITY', 'table ' .. r.label .. ': identity column ' .. table.concat(ident, ', ') .. ' is migrated as a plain column carrying its values (no IDENTITY in Exasol - inserts must supply the value)', false) end
			if #comp > 0 then note('COMPUTED COLUMN', 'table ' .. r.label .. ': computed column(s) ' .. table.concat(comp, ', ') .. ' are migrated as plain columns with their current values (the formula is not migrated)' .. ((G_CHECK and #compv > 0) and ('; CHECK_MIGRATION does not compare the non-persisted one(s) ' .. table.concat(compv, ', ') .. ' (SQL Server computes them on every read, possibly with a different value)') or ''), false) end
			if #reserved > 0 then note('RESERVED NAME', 'table ' .. r.label .. ' has columns whose names are reserved words in Exasol and must be quoted in queries: ' .. table.concat(reserved, ', '), false) end
			if RESERVED[r.exa_table:upper()] then note('RESERVED NAME', 'the table name ' .. oneline(r.exa_table) .. ' (' .. r.label .. ') is a reserved word in Exasol and must be quoted in queries', false) end
		end
		r.colmap = {}
		for _, col in ipairs(r.cols) do r.colmap[col.name] = col end
	end
end
do
	local any_geo, any_trunc = false, false
	for _, r in ipairs(RELS) do
		if r.migrate and r.has_geo then any_geo = true end
		if r.migrate and TRUNC then for _, col in ipairs(r.cols) do if col.m and col.m.structured then any_trunc = true end end end
	end
	if any_geo then note('GEOMETRY', 'geometry/geography values are migrated as 2-D WKT: Z and M values and SRIDs are not kept, curves (CIRCULARSTRING, COMPOUNDCURVE, CURVEPOLYGON) become line approximations (STCurveToLine), a geography FULLGLOBE becomes NULL', false) end
	if any_trunc then note('TRUNCATE', 'TRUNCATE_LONG_STRINGS = true cuts long xml / json / vector values in the middle - such a value is no longer valid', true) end
end

-------------------------------------------------------------------------------------------------------------
-- primary keys (and the unique clustered index of a migrated indexed view)
-------------------------------------------------------------------------------------------------------------
local PK = {}
do
	local pr = remote_all(function(db, B)
		local q = 'select ' .. nl_(db) .. ' as DBN, kc.parent_object_id as OID, kc.name collate database_default as PKN, col.name collate database_default as CN, cast(ic.key_ordinal as int) as ORD, cast(i.is_disabled as int) as DIS ' ..
		'from ' .. B .. '.sys.key_constraints kc join ' .. B .. '.sys.indexes i on i.object_id = kc.parent_object_id and i.index_id = kc.unique_index_id ' ..
		'join ' .. B .. '.sys.index_columns ic on ic.object_id = i.object_id and ic.index_id = i.index_id and ic.key_ordinal > 0 ' ..
		'join ' .. B .. '.sys.columns col on col.object_id = ic.object_id and col.column_id = ic.column_id ' ..
		'join ' .. objfrom(B) .. ' on o.object_id = kc.parent_object_id where kc.type = ' .. sl('PK') .. ' and ' .. OBJW
		if G_IV then
			q = q .. ' union all select ' .. nl_(db) .. ', i.object_id, i.name, col.name, cast(ic.key_ordinal as int), cast(0 as int) ' ..
			'from ' .. B .. '.sys.indexes i join ' .. B .. '.sys.index_columns ic on ic.object_id = i.object_id and ic.index_id = i.index_id and ic.key_ordinal > 0 ' ..
			'join ' .. B .. '.sys.columns col on col.object_id = ic.object_id and col.column_id = ic.column_id ' ..
			'join ' .. objfrom(B) .. ' on o.object_id = i.object_id where o.type = ' .. sl('V') .. ' and i.index_id = 1 and i.is_unique = 1 and ' .. OBJW
		end
		return q
	end)
	for i = 1, #pr do
		local r = REL_BY[rkey(pr[i][1], int(pr[i][2]))]
		if r and r.migrate then
			PK[r] = PK[r] or {name = pr[i][3], cols = {}, disabled = int(pr[i][6]) == 1}
			PK[r].cols[int(pr[i][5])] = pr[i][4]
		end
	end
end

-------------------------------------------------------------------------------------------------------------
-- foreign keys: only onto the parent's primary key, parent migrated, columns migrated; types widened when lossless
-------------------------------------------------------------------------------------------------------------
local FKS, FK_ORDER = {}, {}
do
	local fr = remote_all(function(db, B)
		return 'select ' .. nl_(db) .. ' as DBN, fk.object_id as FKID, fk.name collate database_default as FKN, fk.parent_object_id as COID, fk.referenced_object_id as POID, ' ..
		'cast(fk.is_disabled as int) as DIS, cast(fk.is_not_trusted as int) as NTR, cp.name collate database_default as CCOL, cr.name collate database_default as PCOL, cast(fkc.constraint_column_id as int) as ORD, ' ..
		'cast(pkc.key_ordinal as int) as PKPOS, ' ..
		'cast((select count(*) from ' .. B .. '.sys.index_columns x where x.object_id = fk.referenced_object_id and x.index_id = pk.unique_index_id and x.key_ordinal > 0) as int) as PKLEN, ' ..
		'(select count(*) from ' .. B .. '.sys.foreign_key_columns y where y.constraint_object_id = fk.object_id) as FKLEN, ' ..
		'rs.name collate database_default as PSCH, ro.name collate database_default as PTN ' ..
		'from ' .. B .. '.sys.foreign_keys fk join ' .. B .. '.sys.foreign_key_columns fkc on fkc.constraint_object_id = fk.object_id ' ..
		'join ' .. B .. '.sys.columns cp on cp.object_id = fk.parent_object_id and cp.column_id = fkc.parent_column_id ' ..
		'join ' .. B .. '.sys.columns cr on cr.object_id = fk.referenced_object_id and cr.column_id = fkc.referenced_column_id ' ..
		'join ' .. B .. '.sys.objects ro on ro.object_id = fk.referenced_object_id join ' .. B .. '.sys.schemas rs on rs.schema_id = ro.schema_id ' ..
		'left join ' .. B .. '.sys.key_constraints pk on pk.parent_object_id = fk.referenced_object_id and pk.type = ' .. sl('PK') .. ' and pk.unique_index_id = fk.key_index_id ' ..
		'left join ' .. B .. '.sys.index_columns pkc on pkc.object_id = fk.referenced_object_id and pkc.index_id = pk.unique_index_id and pkc.column_id = fkc.referenced_column_id ' ..
		'join ' .. objfrom(B) .. ' on o.object_id = fk.parent_object_id where ' .. OBJW
	end)
	for i = 1, #fr do
		local x = fr[i]
		local key = x[1] .. NL .. istr(int(x[2]))
		local f = FKS[key]
		if not f then
			f = {db = x[1], name = x[3], coid = int(x[4]), poid = int(x[5]), disabled = int(x[6]) == 1, untrusted = int(x[7]) == 1,
			     pklen = int(x[12]), fklen = int(x[13]), psch = x[14], ptn = x[15], pairs = {}}
			FKS[key] = f; FK_ORDER[#FK_ORDER + 1] = key
		end
		f.pairs[#f.pairs + 1] = {ccol = x[8], pcol = x[9], pkpos = int(x[11])}
	end
	table.sort(FK_ORDER, function(a, b)
		local fa, fb = FKS[a], FKS[b]
		if fa.db ~= fb.db then return fa.db < fb.db end
		if fa.coid ~= fb.coid then return fa.coid < fb.coid end
		return fa.name < fb.name
	end)
end

local function widen(ct, pt)
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
local function fk_reason(f, cr_, pr_)
	if not pr_ or not pr_.migrate then return 'its parent table ' .. oneline(f.db) .. '.' .. oneline(f.psch) .. '.' .. oneline(f.ptn) .. ' is not migrated' end
	if f.pklen == 0 or f.pklen ~= f.fklen then return 'it does not reference the primary key of its parent (Exasol foreign keys always reference the primary key)' end
	for _, p in ipairs(f.pairs) do
		if p.pkpos == nil then return 'it does not reference the primary key of its parent (Exasol foreign keys always reference the primary key)' end
		local cc, pc = cr_.colmap[p.ccol], pr_.colmap[p.pcol]
		if not cc or not pc or not cc.exa or not pc.exa then return 'a key column is not migrated' end
	end
	return nil
end
do
	local changed, rounds = true, 0
	while changed and rounds < #FK_ORDER + 2 do
		changed, rounds = false, rounds + 1
		for _, key in ipairs(FK_ORDER) do
			local f = FKS[key]
			local cr_, pr_ = REL_BY[rkey(f.db, f.coid)], REL_BY[rkey(f.db, f.poid)]
			if cr_ and cr_.migrate and not fk_reason(f, cr_, pr_) then
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
			if col.newtyp then note('WIDENED COLUMN', 'column ' .. r.label .. '.' .. oneline(col.name) .. ' is created as ' .. col.newtyp .. ' (the type of the referenced primary key) instead of ' .. col.m.typ, false) end
		end
	end
end
local FK_OUT, FK_SIG = {}, {}
for _, key in ipairs(FK_ORDER) do
	local f = FKS[key]
	local cr_, pr_ = REL_BY[rkey(f.db, f.coid)], REL_BY[rkey(f.db, f.poid)]
	if cr_ and cr_.migrate then
		local flabel = cr_.label .. ' (' .. oneline(f.name) .. ')'
		local why = fk_reason(f, cr_, pr_)
		if not why then
			table.sort(f.pairs, function(a, b) return a.pkpos < b.pkpos end)
			for _, p in ipairs(f.pairs) do
				local cc, pc = cr_.colmap[p.ccol], pr_.colmap[p.pcol]
				local ct, pt = cc.newtyp or cc.m.typ, pc.newtyp or pc.m.typ
				if ct ~= pt then why = 'the column types differ (' .. ct .. ' / ' .. pt .. ') and cannot be widened losslessly' break end
			end
			if not why then
				local sig = {istr(f.coid), istr(f.poid), f.db}
				for _, p in ipairs(f.pairs) do sig[#sig + 1] = p.ccol end
				sig = table.concat(sig, NL)
				local kept = FK_SIG[sig]
				if kept then
					kept.disabled = kept.disabled and f.disabled
					kept.untrusted = kept.untrusted and f.untrusted
					why = 'it is identical to foreign key ' .. oneline(kept.name) .. ' (same columns and parent; Exasol allows only one)'
				else
					FK_SIG[sig] = f; f.exa_name = f.name; f.cr, f.pr = cr_, pr_
					FK_OUT[#FK_OUT + 1] = f
				end
			end
		end
		if why then note('SKIPPED FOREIGN KEY', 'foreign key ' .. flabel .. ' is not migrated because ' .. why, true) end
	end
end
if ICI and #FK_OUT > 0 then
	local parts = {}
	for i, f in ipairs(FK_OUT) do parts[#parts + 1] = 'select ' .. istr(i) .. ' as i, upper(' .. sl(f.name) .. ') as u from sys.dual' end
	for k = 1, #parts, 500 do
		local ur = query('select i, u from (' .. table.concat(parts, ' union all ', k, math.min(k + 499, #parts)) .. ')')
		for j = 1, #ur do FK_OUT[int(ur[j][1])].exa_name = ur[j][2] end
	end
end
table.sort(FK_OUT, function(a, b)
	if a.cr.exa_schema ~= b.cr.exa_schema then return a.cr.exa_schema < b.cr.exa_schema end
	if a.cr.exa_table ~= b.cr.exa_table then return a.cr.exa_table < b.cr.exa_table end
	if a.exa_name ~= b.exa_name then return a.exa_name < b.exa_name end
	return a.name < b.name
end)
local function pk_name(r) return usub(r.exa_table, 125) .. '_PK' end
do
	local seen = {}
	for r in pairs(PK) do seen[tostring(r) .. NL .. pk_name(r)] = true end
	for _, f in ipairs(FK_OUT) do
		f.exa_name = usub(f.exa_name, 128)
		local base, n = usub(f.exa_name, 120), 1
		while seen[tostring(f.cr) .. NL .. f.exa_name] do n = n + 1; f.exa_name = base .. '_' .. istr(n) end
		seen[tostring(f.cr) .. NL .. f.exa_name] = true
	end
end

-- keys on character columns: SQL Server compares them case-insensitively (usually) and ignores trailing blanks; Exasol
-- compares binary, so enabling such a key can fail and joins on it can return fewer rows
do
	local tabs, seen = {}, {}
	local function chk(r, cname)
		local col = r and r.colmap[cname]
		if col and col.m and (col.m.cls == 'char' or col.m.cls == 'text') and not seen[r] then seen[r] = true; tabs[#tabs + 1] = r.label end
	end
	for r, pk in pairs(PK) do for _, cn in pairs(pk.cols) do chk(r, cn) end end
	for _, f in ipairs(FK_OUT) do for _, p in ipairs(f.pairs) do chk(f.cr, p.ccol); chk(f.pr, p.pcol) end end
	table.sort(tabs)
	if #tabs > 0 then
		note('CASE-INSENSITIVE KEY', 'primary or foreign keys on character columns (' .. table.concat(tabs, ', ') .. ') - SQL Server usually compares them case-insensitively and ignores trailing blanks, Exasol compares binary: ENABLE fails if the data relies on that (also when a key value is an empty string, which is NULL in Exasol), and joins on such keys can return fewer rows', CSTATE ~= 'FORCE_DISABLE')
	end
end

-------------------------------------------------------------------------------------------------------------
-- partitions: the SQL Server partitioning column, when its Exasol type can be partitioned
-------------------------------------------------------------------------------------------------------------
local PARTS = {}
if G_PART then
	for _, r in ipairs(RELS) do
		if r.migrate and r.pf and r.pcol then
			local col = r.colmap[r.pcol]
			local t = (col and col.exa) and (col.newtyp or col.m.typ) or ''
			local ok = t:match('^DECIMAL') or t:match('^DOUBLE') or t:match('^DATE') or t:match('^TIMESTAMP') or t:match('^BOOLEAN') or t:match('^HASHTYPE')
			if ok then
				PARTS[#PARTS + 1] = 'ALTER TABLE ' .. qi(r.exa_schema) .. '.' .. qi(r.exa_table) .. ' PARTITION BY ' .. qi(col.exa) .. ';'
			else
				note('PARTITION', 'table ' .. r.label .. ' (Exasol ' .. oneline(r.exa_schema) .. '.' .. oneline(r.exa_table) .. ') - SQL Server partitioning column ' .. oneline(r.pcol) ..
				     ' maps to ' .. ((t ~= '') and t or 'a column that is not migrated') .. ', which Exasol cannot partition on (numeric, date, timestamp, boolean or hashtype only); add PARTITION BY manually if appropriate', false)
			end
		end
	end
	table.sort(PARTS)
end

-------------------------------------------------------------------------------------------------------------
-- comments (MS_Description); Exasol accepts at most 2,000 characters
-------------------------------------------------------------------------------------------------------------
local COMMENTS, VIEW_COMMENTS = {}, {}
local function ctext(txt, what)
	if ulen(txt) > 2000 then
		note('COMMENT CUT', 'the comment of ' .. what .. ' has ' .. istr(ulen(txt)) .. ' characters and is cut to 2,000 (Exasol limit)', false)
		return usub(txt, 1996) .. ' ...'
	end
	return txt
end
if G_COMM or G_VIEWS then
	local cw = filt(SF, 's3.name')
	local qr = remote_all(function(db, B)
		return 'select ' .. nl_(db) .. ' as DBN, cast(ep.class as int) as CL, ep.major_id as MAJ, cast(ep.minor_id as int) as MINR, s3.name collate database_default as SNAME, ' ..
		'c.name collate database_default as CN, case when cast(sql_variant_property(ep.value, ' .. sl('BaseType') .. ') as nvarchar(128)) in (N' .. sl('varchar') .. ', N' .. sl('char') .. ') ' ..
		'and collationproperty(cast(sql_variant_property(ep.value, ' .. sl('Collation') .. ') as nvarchar(128)), ' .. sl('CodePage') .. ') = ' ..
		'collationproperty(cast(databasepropertyex(db_name(), ' .. sl('Collation') .. ') as nvarchar(128)), ' .. sl('CodePage') .. ') then cast(cast(ep.value as varchar(8000)) as nvarchar(max)) else cast(ep.value as nvarchar(4000)) end as TXT ' ..
		'from ' .. B .. '.sys.extended_properties ep left join ' .. B .. '.sys.schemas s3 on ep.class = 3 and s3.schema_id = ep.major_id ' ..
		'left join ' .. B .. '.sys.columns c on ep.class = 1 and ep.minor_id > 0 and c.object_id = ep.major_id and c.column_id = ep.minor_id ' ..
		'where ep.name = ' .. sl('MS_Description') .. ' and ((ep.class = 3 and s3.name is not null and ' .. cw .. ') or (ep.class = 1 and ep.major_id in ' ..
		'(select o.object_id from ' .. objfrom(B) .. ' where o.type in (' .. sl('U') .. ', ' .. sl('V') .. ') and ' .. OBJW .. ')))'
	end)
	-- schema comments only where exactly one source schema maps to the target schema and TARGET_SCHEMA / DB2SCHEMA are not used
	local src_of = {}
	for _, r in ipairs(RELS) do
		if r.migrate then
			src_of[r.exa_schema] = src_of[r.exa_schema] or {}
			src_of[r.exa_schema][r.db .. NL .. r.sch] = true
		end
	end
	local schema_done = {}
	for i = 1, #qr do
		local db, cl, maj, minr, sname, cn, txt = qr[i][1], int(qr[i][2]), int(qr[i][3]), int(qr[i][4]), str(qr[i][5]), str(qr[i][6]), str(qr[i][7])
		if txt then txt = txt:gsub(string.char(0), ' ') end
		if txt and txt ~= '' then
			if cl == 3 then
				if G_COMM and not TGT and not D2S then
					for _, r in ipairs(RELS) do
						if r.migrate and r.db == db and r.sch == sname and not schema_done[r.exa_schema] then
							local n = 0
							for _ in pairs(src_of[r.exa_schema]) do n = n + 1 end
							schema_done[r.exa_schema] = true
							if n == 1 then COMMENTS[#COMMENTS + 1] = {r.exa_schema, '', 0, 'COMMENT ON SCHEMA ' .. qi(r.exa_schema) .. ' IS ' .. sl(ctext(txt, 'schema ' .. oneline(db) .. '.' .. oneline(sname))) .. ';'} end
						end
					end
				end
			else
				local r = REL_BY[rkey(db, maj)]
				if r and r.migrate and G_COMM then
					if minr == 0 then
						COMMENTS[#COMMENTS + 1] = {r.exa_schema, r.exa_table, 0, 'COMMENT ON TABLE ' .. qi(r.exa_schema) .. '.' .. qi(r.exa_table) .. ' IS ' .. sl(ctext(txt, 'table ' .. r.label)) .. ';'}
					else
						local col = cn and r.colmap[cn]
						if col and col.exa then
							COMMENTS[#COMMENTS + 1] = {r.exa_schema, r.exa_table, col.num, 'COMMENT ON COLUMN ' .. qi(r.exa_schema) .. '.' .. qi(r.exa_table) .. '.' .. qi(col.exa) .. ' IS ' .. sl(ctext(txt, 'column ' .. r.label .. '.' .. oneline(cn))) .. ';'}
						end
					end
				elseif not r then
					local k = db .. NL .. istr(maj)
					VIEW_COMMENTS[k] = VIEW_COMMENTS[k] or {}
					VIEW_COMMENTS[k][#VIEW_COMMENTS[k] + 1] = ((minr == 0) and 'comment' or ('column ' .. oneline(cn or '') .. ' comment')) .. ' - ' .. txt
				end
			end
		end
	end
	table.sort(COMMENTS, function(a, b) if a[1] ~= b[1] then return a[1] < b[1] end if a[2] ~= b[2] then return a[2] < b[2] end if a[3] ~= b[3] then return a[3] < b[3] end return a[4] < b[4] end)
end

-------------------------------------------------------------------------------------------------------------
-- parallel reading: PARTITION > KEYRANGE (clustered rowstore leading key) > PHYSLOC (heap / columnstore) > SINGLE
-------------------------------------------------------------------------------------------------------------
local INT_RANGE = {tinyint = {0, 255}, smallint = {-32768, 32767}, int = {-2147483648, 2147483647}, bigint = {math.mininteger, math.maxinteger}}
local DATE_KEYS = {date = true, datetime = true, datetime2 = true, smalldatetime = true}
local KEY_OK = {tinyint = true, smallint = true, int = true, bigint = true, decimal = true, numeric = true, money = true, smallmoney = true,
	float = true, real = true, date = true, datetime = true, datetime2 = true, smalldatetime = true, datetimeoffset = true, time = true,
	char = true, varchar = true, nchar = true, nvarchar = true, uniqueidentifier = true, binary = true, varbinary = true, bit = true}

local CAND = {}
if PS > 1 then
	for _, r in ipairs(RELS) do
		if r.migrate and not r.noimport and r.kind == 'U' and not r.mo and r.rows >= PMIN and r.rows > 0 then CAND[#CAND + 1] = r end
	end
end
local function oidlist(db, list)
	local ids = {}
	for _, r in ipairs(list) do if r.db == db then ids[#ids + 1] = istr(r.oid) end end
	return ids
end
-- partition row counts
local PROWS = {}
do
	local plist = {}
	for _, r in ipairs(CAND) do if r.pf and r.pcol and r.colmap[r.pcol] then plist[#plist + 1] = r end end
	if #plist > 0 then
		local pr = remote_all(function(db, B)
			local ids = oidlist(db, plist)
			if #ids == 0 then return 'select ' .. nl_(db) .. ' as DBN, 0 as OID, 0 as PN, 0 as RWS where 1 = 0' end
			local parts = {}
			for i = 1, #ids, 2000 do
				parts[#parts + 1] = 'select ' .. nl_(db) .. ' as DBN, p.object_id as OID, p.partition_number as PN, p.rows as RWS from ' .. B ..
				       '.sys.partitions p where p.index_id in (0, 1) and p.object_id in (' .. table.concat(ids, ', ', i, math.min(i + 1999, #ids)) .. ')'
			end
			return parts
		end)
		for i = 1, #pr do
			local k = rkey(pr[i][1], int(pr[i][2]))
			PROWS[k] = PROWS[k] or {}
			PROWS[k][#PROWS[k] + 1] = {int(pr[i][3]), tonumber(pr[i][4]) or 0}
		end
		for _, v in pairs(PROWS) do table.sort(v, function(a, b) return a[1] < b[1] end) end
	end
end
-- histogram steps of the clustered index leading key (KEYRANGE candidates)
local HIST = {}
local function key_kind(k)
	if not k or not KEY_OK[k.typ] then return nil end
	if INT_RANGE[k.typ] or ((k.typ == 'decimal' or k.typ == 'numeric') and k.scale == 0 and k.prec <= 18) then return 'int' end
	if DATE_KEYS[k.typ] then return 'date' end
	if k.typ == 'datetimeoffset' then return 'dto' end
	if k.typ == 'time' then return 'time' end
	if k.typ == 'float' or k.typ == 'real' then return 'flt' end
	if k.typ == 'decimal' or k.typ == 'numeric' or k.typ == 'money' or k.typ == 'smallmoney' then return 'dec' end
	return 'step'
end
do
	local hlist = {}
	for _, r in ipairs(CAND) do
		if not (r.pf and PROWS[rkey(r.db, r.oid)] and #PROWS[rkey(r.db, r.oid)] >= 2) and r.itype == 1 and key_kind(r.key) then hlist[#hlist + 1] = r end
	end
	if #hlist > 0 and (VMAJ >= 14 or AZURE) then                -- sys.dm_db_stats_histogram: SQL Server 2016 SP1 CU2+; 2016 uses MIN/MAX
		local hr = remote_all(function(db, B)
			local parts = {}
			for _, r in ipairs(hlist) do
				if r.db == db then
					local t, kk = r.key.typ, key_kind(r.key)
					local lit
					if t == 'char' or t == 'varchar' then
						lit = sl('0x') .. ' + convert(varchar(max), cast(cast(h.range_high_key as nvarchar(4000)) as varbinary(8000)), 2)'
					elseif t == 'float' or t == 'real' then
						lit = 'convert(varchar(30), cast(h.range_high_key as float), 3)'
					else
						lit = sl('0x') .. ' + convert(varchar(max), cast(h.range_high_key as varbinary(900)), 2)'
					end
					local num = 'cast(null as varchar(40))'
					if kk == 'int' then num = 'cast(cast(h.range_high_key as decimal(38,0)) as varchar(40))'
					elseif kk == 'date' then num = 'cast(datediff_big(second, cast(' .. sl('00010101') .. ' as datetime2(7)), cast(h.range_high_key as datetime2(7))) as varchar(40))'
					elseif kk == 'dto' then num = 'cast(datediff_big(second, cast(' .. sl('00010101') .. ' as datetime2(7)), cast(switchoffset(cast(h.range_high_key as datetimeoffset(7)), ' .. sl('+00:00') .. ') as datetime2(7))) as varchar(40))'
					elseif kk == 'time' then num = 'cast(datediff(second, cast(' .. sl('00:00:00') .. ' as time(7)), cast(h.range_high_key as time(7))) as varchar(40))'
					elseif kk == 'flt' then num = 'convert(varchar(30), cast(h.range_high_key as float), 3)'
					elseif kk == 'dec' then num = 'cast(cast(h.range_high_key as decimal(38,' .. istr((t == 'money' or t == 'smallmoney') and 4 or r.key.scale) .. ')) as varchar(42))' end
					parts[#parts + 1] = 'select ' .. nl_(db) .. ' as DBN, ' .. istr(r.oid) .. ' as OID, h.step_number as STEP, ' .. lit .. ' as KLIT, ' .. num .. ' as KNUM, ' ..
						'cast(h.range_rows as float) as RR, cast(h.equal_rows as float) as ER from ' .. B .. '.sys.dm_db_stats_histogram(' .. istr(r.oid) .. ', 1) h where h.range_high_key is not null'
				end
			end
			if #parts == 0 then return 'select ' .. nl_(db) .. ' as DBN, 0 as OID, 0 as STEP, ' .. sl('') .. ' as KLIT, ' .. sl('') .. ' as KNUM, cast(0 as float) as RR, cast(0 as float) as ER where 1 = 0' end
			return parts
		end)
		for i = 1, #hr do
			local k = rkey(hr[i][1], int(hr[i][2]))
			HIST[k] = HIST[k] or {}
			HIST[k][#HIST[k] + 1] = {step = int(hr[i][3]), lit = str(hr[i][4]), num = str(hr[i][5]), rr = tonumber(hr[i][6]) or 0, er = tonumber(hr[i][7]) or 0}
		end
		for _, v in pairs(HIST) do table.sort(v, function(a, b) return a.step < b.step end) end
	end
end

local function typespec(k)
	local t = k.typ
	if t == 'varchar' or t == 'char' or t == 'varbinary' or t == 'binary' then return t .. '(' .. istr(k.len) .. ')' end
	if t == 'nvarchar' or t == 'nchar' then return t .. '(' .. istr(math.floor(k.len / 2)) .. ')' end
	if t == 'decimal' or t == 'numeric' then return t .. '(' .. istr(k.prec) .. ',' .. istr(k.scale) .. ')' end
	if t == 'datetime2' or t == 'time' or t == 'datetimeoffset' then return t .. '(' .. istr(k.scale) .. ')' end
	return t
end
local function key_literal(k, lit)
	local t, ts = k.typ, typespec(k)
	if t == 'char' or t == 'varchar' then
		return 'cast(cast(' .. lit .. ' as nvarchar(' .. istr(math.min(k.len, 4000)) .. ')) collate ' .. k.coll .. ' as ' .. ts .. ')'
	elseif t == 'float' or t == 'real' then
		return 'cast(' .. sl(lit) .. ' as ' .. ts .. ')'
	end
	return 'cast(' .. lit .. ' as ' .. ts .. ')'
end
local function interp(steps, n)
	-- piecewise-linear cumulative distribution over the histogram; integer boundaries (floats only for the fraction)
	local pts, cum, prev = {}, 0.0, nil
	for _, s in ipairs(steps) do
		local x = tonumber(s.num)
		if x == nil then return {} end
		if prev ~= nil then pts[#pts + 1] = {prev, cum}; cum = cum + s.rr; pts[#pts + 1] = {x, cum} end
		cum = cum + s.er; prev = x
	end
	if prev == nil then return {} end
	pts[#pts + 1] = {prev, cum}
	local out = {}
	for kx = 1, n - 1 do
		local tgt = kx * cum / n
		for j = 1, #pts - 1 do
			local x0, c0, x1, c1 = pts[j][1], pts[j][2], pts[j + 1][1], pts[j + 1][2]
			if c1 >= tgt and c1 > c0 then
				local xf = x0 + (x1 * 1.0 - x0 * 1.0) * ((tgt - c0) / (c1 - c0)) + 1
				local xi
				if xf >= 9.2e18 then xi = math.maxinteger elseif xf <= -9.2e18 then xi = math.mininteger else xi = math.floor(xf) end
				xi = math.tointeger(xi) or xi
				if #out == 0 or xi > out[#out] then out[#out + 1] = xi end
				break
			end
		end
	end
	return out
end
local function interpf(steps, n)
	local pts, cum, prev = {}, 0.0, nil
	for _, s in ipairs(steps) do
		local x = tonumber(s.num)
		if x == nil then return {} end
		x = x * 1.0
		if prev ~= nil then pts[#pts + 1] = {prev, cum}; cum = cum + s.rr; pts[#pts + 1] = {x, cum} end
		cum = cum + s.er; prev = x
	end
	if prev == nil then return {} end
	pts[#pts + 1] = {prev, cum}
	local out = {}
	for kx = 1, n - 1 do
		local tgt = kx * cum / n
		for j = 1, #pts - 1 do
			local x0, c0, x1, c1 = pts[j][1], pts[j][2], pts[j + 1][1], pts[j + 1][2]
			if c1 >= tgt and c1 > c0 then
				local f = (tgt - c0) / (c1 - c0)
				local d = x1 - x0
				local xf = (d == math.huge or d == -math.huge) and (x0 * (1 - f) + x1 * f) or (x0 + d * f)
				if xf == xf and xf ~= math.huge and xf ~= -math.huge and (#out == 0 or xf > out[#out]) then out[#out + 1] = xf end
				break
			end
		end
	end
	return out
end
local MONEY_MAX = {money = 922337203685477, smallmoney = 214748.3647}
local function dec_text(k, x)
	-- decimal text at the column scale; nil when it does not fit the key type (float rounding at the type limit)
	local sc = (k.typ == 'money' or k.typ == 'smallmoney') and 4 or k.scale
	local txt = string.format('%.' .. istr(sc) .. 'f', x)
	local ip = txt:gsub('^%-', ''):gsub('%..*$', '')
	if MONEY_MAX[k.typ] then
		if math.abs(x) > MONEY_MAX[k.typ] then return nil end
	elseif ((ip == '0') and 0 or #ip) > k.prec - sc then return nil end
	return txt
end
local function weighted(steps, n)
	local total = 0
	for _, s in ipairs(steps) do total = total + s.rr + s.er end
	local out, cum, kx = {}, 0.0, 1
	for idx, s in ipairs(steps) do
		cum = cum + s.rr + s.er
		while kx < n and cum >= kx * total / n do
			if #out == 0 or out[#out] ~= idx then out[#out + 1] = idx end
			kx = kx + 1
		end
	end
	return out
end
local function step_uneven(steps, idxs)
	-- true when the largest estimated key range holds more than twice its fair share (estimate from the histogram)
	local cum, total = {}, 0
	for j, s in ipairs(steps) do total = total + s.rr + s.er; cum[j] = total end
	local prev, big = 0, 0
	for _, idx in ipairs(idxs) do
		local b = cum[idx] - steps[idx].er
		if b - prev > big then big = b - prev end
		prev = b
	end
	if total - prev > big then big = total - prev end
	return total > 0 and big > total * 2 / (#idxs + 1)
end
local MINMAX = {}
local function predicates(r, n)
	-- returns a list of WHERE predicates (nil = single statement) and the method name
	if n <= 1 or r.noimport or r.kind ~= 'U' or r.mo or r.rows < PMIN or r.rows <= 0 then return nil, 'SINGLE' end
	n = math.min(n, math.max(2, r.rows // 1000))                    -- at least about 1,000 rows per STATEMENT
	local B = qb(r.db)
	local parts = PROWS[rkey(r.db, r.oid)]
	if r.pf and parts and #parts >= 2 then
		local total = 0
		for _, p in ipairs(parts) do total = total + p[2] end
		local groups, cur, cum, kx = {}, {}, 0, 1
		for _, p in ipairs(parts) do
			cur[#cur + 1] = p[1]; cum = cum + p[2]
			if total > 0 and cum >= kx * total / n and #groups < n - 1 then groups[#groups + 1] = cur; cur = {}; kx = math.floor(cum * n / total) + 1 end
		end
		if #cur > 0 then groups[#groups + 1] = cur end
		if #groups >= 2 then
			local out = {}
			for _, g in ipairs(groups) do out[#out + 1] = B .. '.$PARTITION.' .. qb(r.pf) .. '(' .. qb(r.pcol) .. ') between ' .. istr(g[1]) .. ' and ' .. istr(g[#g]) end
			return out, 'PARTITION'
		end
	end
	if r.itype == 1 and key_kind(r.key) then
		local k, kk = r.key, key_kind(r.key)
		local steps = HIST[rkey(r.db, r.oid)] or {}
		local lits = {}
		local uneven = false
		local lo, hi
		if kk == 'int' then
			if INT_RANGE[k.typ] then lo, hi = INT_RANGE[k.typ][1], INT_RANGE[k.typ][2]
			else local h = 1; for _ = 1, k.prec do h = h * 10 end; lo, hi = -(h - 1), h - 1 end
		end
		if kk == 'int' and #steps == 0 then
			-- no histogram: equal ranges between MIN and MAX (integer keys only)
			local mk = rkey(r.db, r.oid)
			if MINMAX[mk] == nil then
				local ok, mm = premote('select cast(min(' .. qb(k.col) .. ') as decimal(38,0)) as MN, cast(max(' .. qb(k.col) .. ') as decimal(38,0)) as MX from ' .. r.src)
				MINMAX[mk] = (ok and #mm == 1 and not isnull(mm[1][1])) and {tonumber(mm[1][1]), tonumber(mm[1][2])} or false
			end
			if MINMAX[mk] then
				local a, b = MINMAX[mk][1], MINMAX[mk][2]
				for kx = 1, n - 1 do
					local x = math.floor(a + (b * 1.0 - a * 1.0) * kx / n) + 1
					x = math.tointeger(x) or x
					if (#lits == 0 or x > lits[#lits]) and x > a and x <= b then lits[#lits + 1] = x end
				end
			end
			for i, x in ipairs(lits) do lits[i] = 'cast(' .. istr(math.max(lo, math.min(hi, x))) .. ' as ' .. typespec(k) .. ')' end
		elseif #steps == 0 then
			return nil, 'SINGLE (no histogram)'
		elseif kk == 'int' then
			for _, x in ipairs(interp(steps, n)) do
				x = math.max(lo, math.min(hi, x))
				local s = istr(x)
				if #lits == 0 or lits[#lits] ~= s then lits[#lits + 1] = s end
			end
			for i, s in ipairs(lits) do lits[i] = 'cast(' .. s .. ' as ' .. typespec(k) .. ')' end
		elseif kk == 'date' then
			local seen = {}
			for _, x in ipairs(interp(steps, n)) do
				x = math.max(0, math.min(x, 315537897599))                    -- 0001-01-01 .. 9999-12-31 23:59:59 in seconds
				local l = 'cast(dateadd(second, ' .. istr(x % 86400) .. ', dateadd(day, ' .. istr(x // 86400) .. ', cast(' .. sl('00010101') .. ' as datetime2(7)))) as ' .. typespec(k) .. ')'
				if not seen[l] then seen[l] = true; lits[#lits + 1] = l end
			end
		elseif kk == 'dto' or kk == 'time' then
			local seen = {}
			for _, x in ipairs(interp(steps, n)) do
				local l
				if kk == 'time' then
					x = math.max(0, math.min(x, 86399))
					l = 'cast(dateadd(second, ' .. istr(x) .. ', cast(' .. sl('00:00:00') .. ' as time(7))) as ' .. typespec(k) .. ')'
				else
					x = math.max(0, math.min(x, 315537897599))
					l = 'cast(todatetimeoffset(dateadd(second, ' .. istr(x % 86400) .. ', dateadd(day, ' .. istr(x // 86400) .. ', cast(' .. sl('00010101') .. ' as datetime2(7)))), ' .. sl('+00:00') .. ') as ' .. typespec(k) .. ')'
				end
				if not seen[l] then seen[l] = true; lits[#lits + 1] = l end
			end
		elseif kk == 'flt' or kk == 'dec' then
			local seen = {}
			for _, x in ipairs(interpf(steps, n)) do
				local txt = (kk == 'flt') and string.format('%.17g', x) or dec_text(k, x)
				if txt then
					local l = 'cast(' .. sl(txt) .. ' as ' .. typespec(k) .. ')'
					if not seen[l] then seen[l] = true; lits[#lits + 1] = l end
				end
			end
		end
		if kk == 'dto' or kk == 'time' or kk == 'flt' or kk == 'dec' then
			-- float / whole-second interpolation can collapse dense high-precision keys: keep the step boundaries when they are more
			local wl, seen, wi = {}, {}, {}
			for _, idx in ipairs(weighted(steps, n)) do
				local l = key_literal(k, steps[idx].lit)
				if not seen[l] then seen[l] = true; wl[#wl + 1] = l; wi[#wi + 1] = idx end
			end
			if #wl > #lits then lits = wl; uneven = step_uneven(steps, wi) end
		elseif kk == 'step' then
			local seen, idxs = {}, {}
			for _, idx in ipairs(weighted(steps, n)) do
				local l = key_literal(k, steps[idx].lit)
				if not seen[l] then seen[l] = true; lits[#lits + 1] = l; idxs[#idxs + 1] = idx end
			end
			-- estimated rows per range: a coarse histogram (e.g. sequential GUID or binary keys: 3 steps) gives one range
			-- with nearly all rows; physical row locations split such a table exactly (every STATEMENT scans the table)
			local cum, total = {}, 0
			for j, s in ipairs(steps) do total = total + s.rr + s.er; cum[j] = total end
			local prev, big = 0, 0
			for _, idx in ipairs(idxs) do
				local b = cum[idx] - steps[idx].er
				if b - prev > big then big = b - prev end
				prev = b
			end
			if total - prev > big then big = total - prev end
			if total > 0 and big > total / 2 then
				local out = {}
				for i = 0, n - 1 do out[#out + 1] = '((checksum(%%physloc%%) % ' .. istr(n) .. ') + ' .. istr(n) .. ') % ' .. istr(n) .. ' = ' .. istr(i) end
				return out, 'PHYSLOC (uneven key ranges)'
			end
			uneven = total > 0 and big > total * 2 / (#lits + 1)
		end
		if #lits == 0 then return nil, 'SINGLE (one key range)' end
		local c = qb(k.col)
		local out = {'(' .. c .. ' < ' .. lits[1] .. (k.null_ and (' or ' .. c .. ' is null)') or ')')}
		for i = 2, #lits do out[#out + 1] = '(' .. c .. ' >= ' .. lits[i - 1] .. ' and ' .. c .. ' < ' .. lits[i] .. ')' end
		out[#out + 1] = '(' .. c .. ' >= ' .. lits[#lits] .. ')'
		return out, uneven and 'KEYRANGE (uneven)' or 'KEYRANGE'
	end
	if r.itype == 0 or r.itype == 5 then
		local out = {}
		for i = 0, n - 1 do out[#out + 1] = '((checksum(%%physloc%%) % ' .. istr(n) .. ') + ' .. istr(n) .. ') % ' .. istr(n) .. ' = ' .. istr(i) end
		return out, 'PHYSLOC'
	end
	return nil, 'SINGLE (unsupported key)'
end

-------------------------------------------------------------------------------------------------------------
-- generate
-------------------------------------------------------------------------------------------------------------
local SIZE_LIMIT = 120000
local ROW_LIMIT = 1990000          -- one output row is VARCHAR(2000000)
local function from_clause(r)
	return ' from ' .. r.src .. (r.mo and ' with (snapshot)' or '') .. ((r.kind == 'V') and ' with (noexpand)' or '')
end

add('-- ### SQL Server -> Exasol migration generated by ' .. SCRIPT_SCHEMA .. '.' .. exa.meta.script_name .. ' ###')
do
	local dbn = {}
	for _, d in ipairs(DBS) do dbn[#dbn + 1] = oneline(d.name) end
	add('-- source SQL Server ' .. oneline(tostring(VER)) .. ' (' .. oneline(tostring(EDITION)) .. '), databases: ' .. ((#dbn > 0) and table.concat(dbn, ', ') or '(none)') ..
	    '; generated in an Exasol session with TIME_ZONE ' .. SESSION_TZ)
end
add('-- PARALLEL_STATEMENTS = ' .. PS_NOTE .. '; PARALLEL_MIN_ROWS = ' .. istr(PMIN))
if PS_AUTO and not SRC_CPU then note('PARALLEL', 'the SQL Server processor count is not readable (xp_msver is not allowed for the connection user) - AUTO is not capped by it', false) end
if PS > 1 then note('PARALLEL', ((PMIN > 1) and ('tables with at least ' .. istr(PMIN) .. ' rows') or 'all non-empty tables') .. ' are read with up to ' .. istr(PS) .. ' parallel STATEMENTs (partitions, clustered key ranges or physical row locations); each STATEMENT is a separate SQL Server transaction, so the source must not be written during the IMPORTs (otherwise rows can be duplicated or missed) - for a consistent copy run the migration from a database snapshot; verify with CHECK_MIGRATION', true) end

local migrated = {}
for _, r in ipairs(RELS) do if r.migrate then migrated[#migrated + 1] = r end end

for _, r in ipairs(migrated) do
	-- column definitions
	r.defs = {}
	local empty_note = false
	for _, col in ipairs(r.cols) do
		if col.exa then
			local m = col.m
			local typ = col.newtyp or m.typ
			local d, why = map_default(col, m)
			if d == nil then
				d = ''
				note('SKIPPED DEFAULT', 'column ' .. r.label .. '.' .. oneline(col.name) .. ' default ' .. oneline(why) .. ' is not migrated (not representable for ' .. typ .. ')', false)
			end
			local nn = ''
			local nn_ok = m.cls == 'dec' or m.cls == 'dbl' or m.cls == 'date' or m.cls == 'ts' or (m.cls == 'tsz' and OORMODE ~= 'NULL') or m.cls == 'hash' or m.cls == 'uuid'
			if not col.nullable and nn_ok then nn = ' NOT NULL' end
			if not col.nullable and not nn_ok then if m.cls == 'tsz' then r.nn_oor = true else empty_note = true end end
			r.defs[#r.defs + 1] = qi(col.exa) .. ' ' .. typ .. d .. nn
			r.defs0 = r.defs0 or {}; r.defs0[#r.defs0 + 1] = qi(col.exa) .. ' ' .. typ .. nn
		end
	end
	if empty_note then r.nn_dropped = true end
	do local tl = 0; for _, x in ipairs(r.defs) do tl = tl + #x + 2 end
		if tl > ROW_LIMIT - 1000 then r.defs = r.defs0; note('SIZE LIMIT', 'the CREATE TABLE of ' .. r.label .. ' would exceed one output row of 2,000,000 characters - its DEFAULTs are not migrated', true) end end
	-- primary key
	local pk = PK[r]
	if pk then
		local cols, ok = {}, true
		for i = 1, #pk.cols do
			local col = r.colmap[pk.cols[i]]
			if not col or not col.exa then ok = false; note('SKIPPED PRIMARY KEY', 'primary key of ' .. r.label .. ' is not migrated because its column ' .. oneline(pk.cols[i] or '') .. ' is not migrated', true) break end
			cols[#cols + 1] = qi(col.exa)
			if col and col.m and col.m.cls == 'tsz' and OORMODE ~= 'FAIL' and not r.pk_oor_noted then
				r.pk_oor_noted = true
				note('PRIMARY KEY', 'primary key of ' .. r.label .. ' contains the datetimeoffset column ' .. oneline(col.name) .. ': under TEMPORAL_OUT_OF_RANGE = ' .. OORMODE ..
				     ' values outside 0001-01-02 .. 9999-12-30 UTC become ' .. ((OORMODE == 'NULL') and 'NULL' or 'equal to the range limits') .. ' - the key (and foreign keys referencing it) cannot be enabled when such values exist', true)
			end
		end
		if ok and #cols > 0 then
			r.pk_name = pk_name(r)
			r.pk_disabled = pk.disabled
			r.pk_line = 'ALTER TABLE ' .. qi(r.exa_schema) .. '.' .. qi(r.exa_table) .. ' ADD CONSTRAINT ' .. qi(r.pk_name) .. ' PRIMARY KEY (' .. table.concat(cols, ', ') .. ') DISABLE;'
		end
	end
	-- IMPORT
	if not r.noimport then
		local tcols, srcs = {}, {}
		for _, col in ipairs(r.cols) do
			if col.exa then tcols[#tcols + 1] = qi(col.exa); srcs[#srcs + 1] = col.m.src end
		end
		local base = 'select ' .. table.concat(srcs, ', ') .. from_clause(r)
		local head = 'IMPORT INTO ' .. qi(r.exa_schema) .. '.' .. qi(r.exa_table) .. ' (' .. table.concat(tcols, ', ') .. ') FROM JDBC AT ' .. CONN
		local function build(n)
			local preds, how = predicates(r, n)
			local raws = {}
			if preds then for _, p in ipairs(preds) do raws[#raws + 1] = base .. ' where ' .. p end else raws[1] = base end
			local stmts, len, maxl, rawmax = {}, #head + 80, 0, 0
			for _, raw in ipairs(raws) do
				local q = sl(raw)
				stmts[#stmts + 1] = q
				len = len + #q + 11
				if #q > maxl then maxl = #q end
				if #raw > rawmax then rawmax = #raw end
			end
			return stmts, len, maxl, rawmax, how
		end
		local stmts, len, maxl, rawmax, how = build(PS)
		local reduced = false
		if #stmts > 1 and len > ROW_LIMIT then
			local nmax = math.max(1, math.floor((ROW_LIMIT - #head - 80) / (maxl + 11)))
			stmts, len, maxl, rawmax, how = build(math.min(nmax, #stmts - 1))
			reduced = true
			note('PARALLEL', 'table ' .. r.label .. ' is read with ' .. istr(#stmts) .. ' instead of up to ' .. istr(PS) .. ' parallel STATEMENTs: every STATEMENT repeats the full select list, and its IMPORT must fit into one output row of 2,000,000 characters', false)
		end
		if rawmax >= SIZE_LIMIT then
			note('SIZE LIMIT', 'the longest STATEMENT of the IMPORT of ' .. r.label .. ' is ' .. istr(rawmax) .. ' bytes long; Exasol rejects a STATEMENT of 131072 bytes or more (ETL-1100) - reduce the number of columns or migrate the table in column groups; with many datetimeoffset columns TEMPORAL_OUT_OF_RANGE = NULL gives the shortest expressions (out-of-range values then load as NULL)', true)
		end
		r.method = how
		if PS > 1 and r.rows >= PMIN and r.kind == 'U' and not r.mo and how and how:find('^SINGLE %(') then
			note('PARALLEL', 'table ' .. r.label .. ' is read with one STATEMENT: ' .. how:gsub('^SINGLE %((.*)%)$', '%1') ..
			     ((r.msk and r.msk > 0) and ' - the clustered key column may be masked for the connection user (no UNMASK permission), which hides its statistics' or '') ..
			     ' (parallel reading needs a partition function, a clustered key with a readable statistics histogram, or a heap / columnstore table)', false)
		elseif how == 'PHYSLOC (uneven key ranges)' then
			note('PARALLEL', 'table ' .. r.label .. ': the statistics histogram of its clustered key gives very uneven key ranges (e.g. a sequential uniqueidentifier or binary key) - it is read by physical row locations instead (exact; every STATEMENT scans the table)', false)
		elseif how and (how:find('^KEYRANGE') or how == 'PARTITION') and PS > 1 and not reduced then
			local want = math.min(PS, math.max(2, r.rows // 1000))
			if how == 'PARTITION' and #stmts < (want + 1) // 2 then
				note('PARALLEL', 'table ' .. r.label .. ' is read with ' .. istr(#stmts) .. ' of up to ' .. istr(want) .. ' STATEMENTs: one STATEMENT reads one or more whole partitions, and the table has only that many partitions with rows', false)
			elseif #stmts < (want + 1) // 2 then
				note('PARALLEL', 'table ' .. r.label .. ' is read with ' .. istr(#stmts) .. ' of up to ' .. istr(want) .. ' STATEMENTs: the statistics histogram of its clustered key allows only that many key ranges (few distinct key values, values very close together, or stale statistics)', false)
			elseif how == 'KEYRANGE (uneven)' then
				note('PARALLEL', 'table ' .. r.label .. ': its key ranges are uneven (the clustered key values are very close together, e.g. within one second) - some STATEMENTs read much more than others', false)
			end
		end
		local parts = {}
		for _, s in ipairs(stmts) do parts[#parts + 1] = ' STATEMENT ' .. s end
		r.import_row = head .. table.concat(parts) .. ';' .. (r.has_tsz and ('  -- requires session TIME_ZONE = ' .. SQ .. 'UTC' .. SQ .. ' (see above)') or '')
	end
end
do
	local any = false
	for _, r in ipairs(migrated) do if r.nn_dropped then any = true end end
	if any then note('EMPTY STRING', 'Exasol stores an empty string as NULL: NOT NULL is kept only on numeric, date/time, hashtype and uniqueidentifier columns - character, binary-text, xml/json/vector, time and sql_variant columns are created nullable, and their empty values arrive as NULL', false) end
	for _, r in ipairs(migrated) do if r.nn_oor then note('NOT NULL', 'table ' .. r.label .. ': NOT NULL is not kept on its datetimeoffset columns - TEMPORAL_OUT_OF_RANGE = NULL loads values outside 0001-01-02 .. 9999-12-30 UTC as NULL', false) end end
end

do
	local inset, lost = {}, {}
	for _, r in ipairs(migrated) do inset[r.exa_schema .. NL .. r.exa_table] = true end
	local ok, fk = pquery('select CONSTRAINT_SCHEMA, CONSTRAINT_TABLE, CONSTRAINT_NAME, REFERENCED_SCHEMA, REFERENCED_TABLE from EXA_ALL_CONSTRAINT_COLUMNS where CONSTRAINT_TYPE = ' .. sl('FOREIGN KEY') .. ' and ORDINAL_POSITION = 1')
	if ok then
		for i = 1, #fk do
			if inset[fk[i][4] .. NL .. fk[i][5]] and not inset[fk[i][1] .. NL .. fk[i][2]] then
				lost[#lost + 1] = oneline(fk[i][1] .. '.' .. fk[i][2] .. ' (' .. fk[i][3] .. ' -> ' .. fk[i][4] .. '.' .. fk[i][5] .. ')')
			end
		end
	end
	table.sort(lost)
	if #lost > 0 then note('FOREIGN KEYS DROPPED', 'DROP TABLE ... CASCADE CONSTRAINTS removes these foreign keys of Exasol tables that are NOT part of this output (state at generation time) - re-create them afterwards: ' .. table.concat(lost, ', '), true) end
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
			local cc, pc = {}, {}
			for _, p in ipairs(f.pairs) do cc[#cc + 1] = qi(f.cr.colmap[p.ccol].exa); pc[#pc + 1] = qi(f.pr.colmap[p.pcol].exa) end
			add('ALTER TABLE ' .. qi(f.cr.exa_schema) .. '.' .. qi(f.cr.exa_table) .. ' ADD CONSTRAINT ' .. qi(f.exa_name) .. ' FOREIGN KEY (' ..
			    table.concat(cc, ', ') .. ') REFERENCES ' .. qi(f.pr.exa_schema) .. '.' .. qi(f.pr.exa_table) .. ' (' .. table.concat(pc, ', ') .. ') DISABLE;')
		end
	end

	if #PARTS > 0 then add('-- ### PARTITION BY ###'); for _, l in ipairs(PARTS) do add(l) end end
	if #COMMENTS > 0 then add('-- ### COMMENTS ###'); for _, c in ipairs(COMMENTS) do add(c[4]) end end

	local any_import = false
	for _, r in ipairs(migrated) do if r.import_row then any_import = true end end
	if not any_import then add('-- ### IMPORTS ###') end
	if any_import then
		add('-- ##########################################################################################' .. NL ..
		    '-- !!! IMPORTANT - TIME ZONE !!!' .. NL ..
		    '-- !!! Run the next ALTER SESSION and ALL IMPORT statements below in the SAME session and in' .. NL ..
		    '-- !!! this order. datetimeoffset values are transferred as UTC and are stored correctly ONLY' .. NL ..
		    '-- !!! while the session TIME_ZONE is ' .. SQ .. 'UTC' .. SQ .. '. If you run an IMPORT on its own, execute' .. NL ..
		    '-- !!! ALTER SESSION SET TIME_ZONE = ' .. SQ .. 'UTC' .. SQ .. '; first. The original time zone is restored at the end;' .. NL ..
		    '-- !!! if the run stops at an error, restore it yourself: ALTER SESSION SET TIME_ZONE = ' .. SQ .. SESSION_TZ .. SQ .. ';' .. NL ..
		    '-- ##########################################################################################')
		add('ALTER SESSION SET TIME_ZONE = ' .. sl('UTC') .. ';')
		add('-- ### IMPORTS ###')
	end
	for _, r in ipairs(migrated) do
		if r.import_row then add(r.import_row)
		else add('-- ' .. oneline(r.exa_schema) .. '.' .. oneline(r.exa_table) .. ' - no IMPORT (see the DISABLED INDEX note)') end
	end

	-- CONSTRAINT STATE
	if CSTATE ~= 'FORCE_DISABLE' then
		local lines = {}
		for _, r in ipairs(migrated) do
			if r.pk_name then
				if CSTATE == 'FORCE_ENABLE' or not r.pk_disabled then
					lines[#lines + 1] = 'ALTER TABLE ' .. qi(r.exa_schema) .. '.' .. qi(r.exa_table) .. ' MODIFY CONSTRAINT ' .. qi(r.pk_name) .. ' ENABLE;'
				else
					lines[#lines + 1] = '-- ' .. oneline(r.exa_schema) .. '.' .. oneline(r.exa_table) .. ' primary key ' .. oneline(r.pk_name) .. ' stays DISABLED (disabled in SQL Server)'
				end
			end
		end
		for _, f in ipairs(FK_OUT) do
			if CSTATE == 'FORCE_ENABLE' or not (f.disabled or f.untrusted) then
				lines[#lines + 1] = 'ALTER TABLE ' .. qi(f.cr.exa_schema) .. '.' .. qi(f.cr.exa_table) .. ' MODIFY CONSTRAINT ' .. qi(f.exa_name) .. ' ENABLE;'
			else
				lines[#lines + 1] = '-- ' .. oneline(f.cr.exa_schema) .. '.' .. oneline(f.cr.exa_table) .. ' foreign key ' .. oneline(f.exa_name) .. ' stays DISABLED (' .. (f.disabled and 'disabled' or 'not trusted - WITH NOCHECK') .. ' in SQL Server)'
			end
		end
		if #lines > 0 then
			add('-- ### CONSTRAINT STATE - run after the IMPORTs (' .. CSTATE .. ') ###')
			for _, l in ipairs(lines) do add(l) end
		end
	end

	-- DATA VALIDATION (one summary table per target schema in the script schema; per table DELETE + INSERT chunks)
	if G_CHECK then
		local EXPECT = {TABLE_NAME = 'VARCHAR(600) UTF8', METRIC = 'VARCHAR(300) UTF8', EXASOL_METRIC = 'VARCHAR(2000000) UTF8',
		                SQLSERVER_METRIC = 'VARCHAR(2000000) UTF8', STATUS = 'VARCHAR(10) ASCII'}
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
					if q[i][2] == 'POSTGRES_METRIC' then cols[t].pg = true end
				end
				for t, v in pairs(cols) do if (v.bad or v.n ~= 5) and not v.pg then OLD_LAYOUT[t] = true end end
				for t, v in pairs(cols) do
					if v.pg then add('-- !!! CHECK SUMMARY: the summary table ' .. oneline(SCRIPT_SCHEMA) .. '.' .. oneline(t) .. ' belongs to postgresql_to_exasol (other layout) and is not replaced - the CHECK INSERTs into it fail; use another TARGET_SCHEMA or drop that table') end
				end
			end
		end
		local sums_done, sums_list, started = {}, {}, false
		for _, r in ipairs(migrated) do
			if r.import_row then
				if not started then
					started = true
					add('-- ### DATA VALIDATION (CHECK_MIGRATION) - compares source and target metrics; run after the IMPORTs (TIME_ZONE ' .. SQ .. 'UTC' .. SQ .. ' is set again below so that this section can be re-run on its own) ###')
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
					add('CREATE TABLE IF NOT EXISTS ' .. summary .. ' ("TABLE_NAME" VARCHAR(600) UTF8, "METRIC" VARCHAR(300) UTF8, "EXASOL_METRIC" VARCHAR(2000000) UTF8, "SQLSERVER_METRIC" VARCHAR(2000000) UTF8, "STATUS" VARCHAR(10) ASCII);')
				end
				local tkey = qi(r.exa_schema) .. '.' .. qi(r.exa_table)
				add('DELETE FROM ' .. summary .. ' WHERE "TABLE_NAME" = ' .. sl(tkey) .. ';')
				add('INSERT INTO ' .. summary .. ' VALUES (' .. sl(tkey) .. ', ' .. sl('CHECK') .. ', NULL, NULL, ' .. sl('INCOMPLETE') .. ');')
				-- metrics: {name, Exasol expression, SQL Server expression}
				local M = {{'ROW_CNT', 'cast(count(*) as decimal(36,0))', 'cast(count_big(*) as decimal(36,0))'}}
				if r.kind == 'U' and not r.mo and r.itype ~= 5 then        -- columnstore catalog counts include deleted rows
					M[#M + 1] = {'ROW_CNT_CATALOG', 'cast(count(*) as decimal(36,0))', '(select cast(sum(p.rows) as decimal(36,0)) from ' .. qb(r.db) .. '.sys.partitions p where p.object_id = object_id(' .. nl_(r.src) .. ') and p.index_id in (0, 1))'}
				end
				local DT7 = sl('YYYY-MM-DD HH24:MI:SS.FF7')
				for _, col in ipairs(r.cols) do
					local m = col.m
					if col.exa and not col.volatile then
						local e = qi(col.exa)
						local p = m.src
						local vc = (m.typ:sub(1, 7) == 'VARCHAR') or m.cls == 'hash'
						local pnull = vc and ('case when (' .. p .. ') is null or datalength(' .. p .. ') = 0 then null else 1 end') or p
						M[#M + 1] = {col.exa .. '_NULLS', 'cast(count(case when ' .. e .. ' is null then 1 end) as decimal(36,0))', 'cast(count_big(case when (' .. pnull .. ') is null then 1 end) as decimal(36,0))'}
						if m.distinct then
							M[#M + 1] = {col.exa .. '_DISTINCT', 'cast(count(distinct ' .. e .. ') as decimal(36,0))', 'cast(count_big(distinct ' .. (m.chk or p) .. ') as decimal(36,0))'}
						end
						if m.cls == 'dec' and m.exact then
							local pc = m.chk or p
							local t = 'decimal(36,' .. istr(m.s) .. ')'
							M[#M + 1] = {col.exa .. '_MIN', 'cast(min(' .. e .. ') as ' .. t .. ')', 'cast(min(' .. pc .. ') as ' .. t .. ')'}
							M[#M + 1] = {col.exa .. '_MAX', 'cast(max(' .. e .. ') as ' .. t .. ')', 'cast(max(' .. pc .. ') as ' .. t .. ')'}
							if m.p <= 26 then M[#M + 1] = {col.exa .. '_SUM', 'cast(sum(' .. e .. ') as ' .. t .. ')', 'cast(sum(cast(' .. pc .. ' as decimal(38,' .. istr(m.s) .. '))) as ' .. t .. ')'} end
						end
						if m.capped then
							M[#M + 1] = {col.exa .. '_ROUNDED', 'cast(0 as decimal(36,0))', 'cast(count_big(case when ' .. qb(col.name) .. ' <> round(' .. qb(col.name) .. ', ' .. istr(m.s) .. ') then 1 end) as decimal(36,0))'}
						end
						if m.cls == 'date' then
							M[#M + 1] = {col.exa .. '_MIN', 'to_char(min(' .. e .. '), ' .. sl('YYYY-MM-DD') .. ')', 'convert(varchar(10), min(' .. p .. '), 23)'}
							M[#M + 1] = {col.exa .. '_MAX', 'to_char(max(' .. e .. '), ' .. sl('YYYY-MM-DD') .. ')', 'convert(varchar(10), max(' .. p .. '), 23)'}
						elseif m.cls == 'ts' or m.cls == 'tsz' then
							-- datetime keeps 1/300 s internally: compare at its 3 digits; smalldatetime at whole seconds; datetime2 / offset at 7 digits
							local sfx = (m.cls == 'tsz') and '_UTC' or ''
							local ef, sx
							if col.bt == 'datetime' then ef, sx = sl('YYYY-MM-DD HH24:MI:SS.FF3'), function(a) return 'convert(varchar(23), ' .. a .. ', 121)' end
							elseif col.bt == 'smalldatetime' then ef, sx = sl('YYYY-MM-DD HH24:MI:SS'), function(a) return 'convert(varchar(19), ' .. a .. ', 120)' end
							else ef, sx = DT7, function(a) return 'convert(varchar(27), cast(' .. a .. ' as datetime2(7)), 121)' end end
							M[#M + 1] = {col.exa .. '_MIN' .. sfx, 'to_char(min(' .. e .. '), ' .. ef .. ')', sx('min(' .. p .. ')')}
							M[#M + 1] = {col.exa .. '_MAX' .. sfx, 'to_char(max(' .. e .. '), ' .. ef .. ')', sx('max(' .. p .. ')')}
						end
						if m.cls == 'dbl' then
							M[#M + 1] = {col.exa .. '_MIN', 'min(' .. e .. ')', 'min(' .. p .. ')'}
							M[#M + 1] = {col.exa .. '_MAX', 'max(' .. e .. ')', 'max(' .. p .. ')'}
						end
						if TRUNC and col.bt == 'json' then
							M[#M + 1] = {col.exa .. '_INVALID_JSON', 'cast(count(case when ' .. e .. ' is not null and ' .. e .. ' is not json then 1 end) as decimal(36,0))', 'cast(0 as decimal(36,0))'}
						end
						if m.plain_text and m.typ:sub(1, 7) == 'VARCHAR' then
							local ln = 'case when datalength(' .. p .. ') = 0 then null else len((cast(' .. p .. ' as nvarchar(max))' .. SC_COLL .. ') + N' .. sl('.') .. ') - 1 end'
							M[#M + 1] = {col.exa .. '_MINLEN', 'cast(min(length(' .. e .. ')) as decimal(36,0))', 'cast(min(' .. ln .. ') as decimal(36,0))'}
							M[#M + 1] = {col.exa .. '_MAXLEN', 'cast(max(length(' .. e .. ')) as decimal(36,0))', 'cast(max(' .. ln .. ') as decimal(36,0))'}
						end
					end
				end
				-- chunks: at most 250 metrics and about 90,000 bytes per remote statement
				local chunk, size = {}, 0
				local function flush()
					if #chunk == 0 then return end
					local ex, px, en, ec, pc = {}, {}, {}, {}, {}
					for i, mm in ipairs(chunk) do
						local a = '"M' .. istr(i) .. '"'
						ex[#ex + 1] = mm[2] .. ' as ' .. a
						px[#px + 1] = mm[3] .. ' as M' .. istr(i)
						en[#en + 1] = 'when ' .. istr(i) .. ' then ' .. sl(mm[1])
						ec[#ec + 1] = 'when ' .. istr(i) .. ' then cast(e.' .. a .. ' as varchar(2000000))'
						pc[#pc + 1] = 'when ' .. istr(i) .. ' then cast(p.' .. a .. ' as varchar(2000000))'
					end
					local ssel = 'select ' .. table.concat(px, ', ') .. from_clause(r)
					add('INSERT INTO ' .. summary .. ' ("TABLE_NAME", "METRIC", "EXASOL_METRIC", "SQLSERVER_METRIC", "STATUS") select ' .. sl(tkey) ..
					    ', v.metric, v.ev, v.pv, case when coalesce(v.ev, ' .. sl('~NULL~') .. ') = coalesce(v.pv, ' .. sl('~NULL~') .. ') then ' .. sl('OK') .. ' else ' .. sl('DEVIATION') .. ' end' ..
					    ' from (select case k.i ' .. table.concat(en, ' ') .. ' end as metric, case k.i ' .. table.concat(ec, ' ') .. ' end as ev, case k.i ' .. table.concat(pc, ' ') .. ' end as pv' ..
					    ' from (select ' .. table.concat(ex, ', ') .. ' from ' .. tkey .. ') e' ..
					    ' cross join (select * from (import from jdbc at ' .. CONN .. ' statement ' .. sl(ssel) .. ')) p' ..
					    ' cross join (select level as i from sys.dual connect by level <= ' .. istr(#chunk) .. ') k) v;')
					chunk, size = {}, 0
				end
				for _, mm in ipairs(M) do
					local s = #mm[2] + #mm[3] + 2 * #mm[1] + 120
					if #chunk >= 250 or size + s > 90000 then flush() end
					chunk[#chunk + 1] = mm; size = size + s
				end
				flush()
				-- the marker is removed only when every metric row arrived (a failed chunk leaves the table INCOMPLETE)
				add('DELETE FROM ' .. summary .. ' WHERE "TABLE_NAME" = ' .. sl(tkey) .. ' AND "METRIC" = ' .. sl('CHECK') .. ' AND "STATUS" = ' .. sl('INCOMPLETE') ..
				    ' AND (SELECT COUNT(*) FROM ' .. summary .. ' WHERE "TABLE_NAME" = ' .. sl(tkey) .. ' AND "METRIC" <> ' .. sl('CHECK') .. ') = ' .. istr(#M) .. ';')
			end
		end
		for _, summary in ipairs(sums_list) do
			add('-- review the result with - select * from ' .. oneline(summary) .. ' where "STATUS" in (' .. sl('INCOMPLETE') .. ', ' .. sl('DEVIATION') .. ') order by "TABLE_NAME", "METRIC";')
		end
	end

	add('-- restore the time zone of the session that generated this script')
	add('ALTER SESSION SET TIME_ZONE = ' .. sl(SESSION_TZ) .. ';')
end

-------------------------------------------------------------------------------------------------------------
-- VIEW review section (T-SQL definitions as comments)
-------------------------------------------------------------------------------------------------------------
if G_VIEWS and #VIEWS > 0 then
	add('-- ### VIEWS (T-SQL definitions - commented out, review and adapt to Exasol SQL manually) ###')
	for _, v in ipairs(VIEWS) do
		local head
		if v.idx then head = (v.migrated and v.migrated.migrate) and ('INDEXED VIEW (migrated as a table) ' .. v.label) or ('INDEXED VIEW (not migrated) ' .. v.label)
		else head = 'VIEW ' .. v.label end
		local extra = ''
		for _, c in ipairs(VIEW_COMMENTS[v.db .. NL .. istr(v.oid)] or {}) do
			local piece = NL .. '-- ' .. cmt(c)
			if ulen(extra) + ulen(piece) > 100000 then extra = extra .. NL .. '-- (further comments omitted)' break end
			extra = extra .. piece
		end
		local def = v.def
		if def == nil then
			add('-- ' .. cmt(head) .. extra .. NL .. '-- (definition not available - WITH ENCRYPTION, or no VIEW DEFINITION permission)')
		else
			def = cmt(def)
			local room = ROW_LIMIT - ulen(head) - ulen(extra) - 100
			if ulen(def) > room then
				def = usub(def, math.max(room, 0))
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
for i = 4, #OUT do
	local row = OUT[i][1]
	if row:sub(1, 2) ~= '--' and row:find('&', 1, true) then
		table.insert(OUT, 4, {'-- !!! EXAPLUS: some statements contain the character & - in EXAplus run SET DEFINE OFF; before this script, otherwise & starts a substitution variable and the text is changed'})
		break
	end
end

if #DBS == 0 then add('-- no database matched DB_FILTER (or every matched database was skipped - see the notes)')
elseif #RELS == 0 and #VIEWS > 0 then add('-- no table matched: the filters matched only views' .. (G_VIEWS and ' (see the VIEWS section)' or ''))
elseif #RELS == 0 then add('-- no table matched DB_FILTER / SCHEMA_FILTER / TABLE_FILTER (names or LIKE patterns; case sensitivity follows the source collation)')
elseif #migrated == 0 then add('-- no table is migrated: see the notes' .. (G_VIEWS and ' and the VIEWS section' or '')) end
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
--   * The SQL Server / Azure SQL database must be reachable from this Exasol database.
--   * The connection user must be able to read all tables to migrate (SELECT and VIEW DEFINITION). Row-level security
--     filters rows silently and dynamic data masking masks values - use a login that the security predicates exempt and
--     that has the UNMASK permission.
--   * Use the latest Microsoft JDBC driver (mssql-jdbc 13.6.0 or newer); the legacy jTDS driver is not supported.

-- JDBC driver (install once in BucketFS - the driver and its settings.cfg)
--   * https://mvnrepository.com/artifact/com.microsoft.sqlserver/mssql-jdbc
--   * Microsoft Entra ID authentication additionally needs the azure-identity library with its dependencies
--     (version 1.18.4, a released version - not a beta): https://mvnrepository.com/artifact/com.azure/azure-identity
--   * Driver setup guide - https://docs.exasol.com/db/latest/loading_data/connect_sources/sql_server.htm

-- Create the connection that matches your environment (adjust host, database name and credentials), then run
-- the accompanying test query. Three common variants are shown.

-- 1) SQL Server on premises (SQL Server authentication; use trustServerCertificate=true only for a server with a
--    self-signed certificate)
CREATE OR REPLACE CONNECTION SQLSERVER_JDBC
    TO 'jdbc:sqlserver://sqlserver_host_or_ip:1433;databaseName=my_database;encrypt=true;trustServerCertificate=false;loginTimeout=30;'
    USER 'user'
    IDENTIFIED BY 'password';
SELECT * FROM (IMPORT FROM JDBC AT SQLSERVER_JDBC STATEMENT 'SELECT ''Connection works''');

-- 2) Azure SQL Database / Managed Instance (SQL authentication). Azure SQL Database: one database per connection -
--    databaseName and DB_FILTER must name the same database.
CREATE OR REPLACE CONNECTION AZURE_SQL_JDBC
    TO 'jdbc:sqlserver://myserver.database.windows.net:1433;databaseName=my_database;encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;'
    USER 'user'
    IDENTIFIED BY 'password';
SELECT * FROM (IMPORT FROM JDBC AT AZURE_SQL_JDBC STATEMENT 'SELECT ''Connection works''');

-- 3) Azure SQL with Microsoft Entra ID - service principal (USER = application / client id, IDENTIFIED BY = client secret;
--    needs azure-identity). ActiveDirectoryPassword is deprecated by Microsoft and fails when MFA is enforced.
CREATE OR REPLACE CONNECTION AZURE_SQL_ENTRA_JDBC
    TO 'jdbc:sqlserver://myserver.database.windows.net:1433;databaseName=my_database;encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;authentication=ActiveDirectoryServicePrincipal;'
    USER 'application_client_id'
    IDENTIFIED BY 'client_secret';
SELECT * FROM (IMPORT FROM JDBC AT AZURE_SQL_ENTRA_JDBC STATEMENT 'SELECT ''Connection works''');

-- ===================================================================================================
-- GENERATE THE MIGRATION STATEMENTS (recommended defaults shown)
-- ===================================================================================================
EXECUTE SCRIPT DATABASE_MIGRATION.SQLSERVER_TO_EXASOL(
    'SQLSERVER_JDBC',   -- CONNECTION_NAME: name of the JDBC connection created above
    false,              -- DB2SCHEMA: false (recommended) => "schema"."table"; true => "database"."schema_table" (several databases at once)
    'my_database',      -- DB_FILTER: database name(s) or LIKE pattern(s), comma separated, e.g. 'sales', 'db1, db2', 'dwh%' (system databases only when named exactly)
    '%',                -- SCHEMA_FILTER: schema name(s) or LIKE pattern(s), e.g. 'dbo', 'sales%', '%' (all; '_' and '%' are wildcards)
    '',                 -- TARGET_SCHEMA: Exasol target schema; '' (recommended) => use the source schema (or database) name; '"MySchema"' => this exact name (no upper-casing); at most 128 characters
    '%',                -- TABLE_FILTER: table name(s) or LIKE pattern(s), e.g. 'orders', 'fact%', '%' (all)
    true,               -- IDENTIFIER_CASE_INSENSITIVE: true (recommended) => fold all identifiers to UPPER case; false => keep them as in SQL Server (quoted)
    'AUTO',             -- PARALLEL_STATEMENTS: 'AUTO' (recommended; Exasol VCPU/NODES/2, even, 4..64, at most the SQL Server processor count), a number >= 1, or 1 = one STATEMENT per table. Do not write to the source during the load
    1000000,            -- PARALLEL_MIN_ROWS: tables with fewer rows are read with one STATEMENT (default 1000000); 0 => split every table that can be split
    'FORCE_DISABLE',    -- CONSTRAINT_STATE: 'FORCE_DISABLE' (recommended; keys stay metadata for BI tools), 'SET_AS_SOURCE' (enable keys that are enabled and trusted in SQL Server) or 'FORCE_ENABLE' (Exasol validates all keys)
    true,               -- GENERATE_COMMENTS: true (recommended) => migrate MS_Description comments of schemas, tables and columns
    true,               -- GENERATE_VIEWS: true => list the source views as a commented manual-review section
    false,              -- MIGRATE_INDEXED_VIEWS: false (default) => indexed views are only listed; true => migrate their stored rows as tables (a snapshot - Exasol does not maintain them)
    true,               -- GENERATE_PARTITION_BY: true => PARTITION BY from the SQL Server partitioning column when its Exasol type allows it
    'HASHTYPE',         -- BINARY_HANDLING: 'HASHTYPE' (recommended; binary(n <= 1024) and rowversion -> HASHTYPE, other binary -> hex text), 'HEX' (all binary as hex text) or 'SKIP' (binary columns are not migrated)
    'CAP',              -- DECIMAL_OVERFLOW: 'CAP' (recommended; decimal(p > 36, s) -> DECIMAL(36, s), a scale above 35 is rounded to 35; values with more than 36 - s integer digits fail), 'DOUBLE' (nearest double, about 15 digits) or 'VARCHAR' (lossless text)
    false,              -- TRUNCATE_LONG_STRINGS: false (recommended) => the IMPORT fails on a value > 2,000,000 characters; true => cut such values (xml/json may become invalid)
    'FAIL',             -- TEMPORAL_OUT_OF_RANGE: 'FAIL' (recommended; the IMPORT fails on a datetimeoffset value outside 0001-01-02 .. 9999-12-30 UTC), 'NULL' (load NULL) or 'CLAMP' (clamp to that range)
    false               -- CHECK_MIGRATION: true => also generate the data validation (summary table <schema>_MIG_CHK in the script schema)
);
