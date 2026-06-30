create schema if not exists database_migration;

/*
    SQLSERVER_TO_EXASOL - generate the statements to migrate a Microsoft SQL Server (or Azure SQL) database
    to Exasol.

    The script runs on the TARGET Exasol database, reads the SOURCE metadata through a JDBC connection and
    RETURNS the statements (CREATE SCHEMA / CREATE TABLE incl. PRIMARY KEY / FOREIGN KEY / PARTITION BY /
    COMMENTs / IMPORT / a final CONSTRAINT STATE section / optional VIEW review). It changes nothing itself -
    review and run the generated statements in the order returned.

    SUPPORTED SOURCE / TARGET
      Microsoft SQL Server 2016, 2017, 2019, 2022 and 2025 (incl. the new native json and vector types) and
      Azure SQL -> Exasol v8 (2025+). Always use the LATEST Microsoft JDBC driver (mssql-jdbc):
        https://mvnrepository.com/artifact/com.microsoft.sqlserver/mssql-jdbc
      Do NOT use the obsolete jTDS driver. (Validated with mssql-jdbc 13.4.0 against SQL Server 2025
      17.0.4055 -> Exasol 2025.1.11.) See the connection examples at the end of this file.

    USER-DEFINED TYPES
      SQL Server alias types (CREATE TYPE ... FROM <base type>) are migrated automatically as their base
      type (e.g. an alias over nvarchar(25) becomes VARCHAR(25)). CLR / assembly user-defined types cannot be
      represented in Exasol: such columns are SKIPPED and listed in a prominent "UNSUPPORTED TYPE" warning at
      the very top of the generated output (any unknown future type is handled the same way).

    DATA-TYPE MAPPING (driven by the BASE system type, so alias types resolve automatically and new built-in
    types need no maintenance):

      Exact (no loss):
        tinyint/smallint/int/bigint -> DECIMAL(3|5|10|19,0); bit -> DECIMAL(1,0);
        decimal/numeric(p,s) -> DECIMAL(p,s) for p <= 36; money/smallmoney -> DECIMAL(19,4)/(10,4);
        char/varchar/nchar/nvarchar/text/ntext -> CHAR/VARCHAR (see "Character types"); date -> DATE;
        datetime -> TIMESTAMP(3); datetime2(n) -> TIMESTAMP(n) (full fractional precision, 0-7 digits);
        uniqueidentifier -> CHAR(36). NOT NULL, IDENTITY and column DEFAULTs are migrated.

      Mapped with a small, documented difference:
        - float/real -> DOUBLE (Exasol has only DOUBLE; ~15 significant digits).
        - smalldatetime -> TIMESTAMP(0) (source resolution is 1 minute).
        - datetimeoffset(n) -> TIMESTAMP(n) WITH LOCAL TIME ZONE: the value is converted to the UTC instant;
          the numeric offset (+02:00 ...) is normalized away, the instant itself is preserved.
        - time(n) -> VARCHAR(16): Exasol has no standalone TIME type, so the time-of-day is kept verbatim as
          text 'HH:MI:SS.fffffff' (no artificial date is invented).
        - rowversion/timestamp -> the 8-byte value as HASHTYPE / hex (a row-version counter, NOT a time).
        - binary/varbinary/image -> binary data preserved as HASHTYPE or hex (see BINARY_HANDLING).
        - xml/json/vector/sql_variant -> VARCHAR (their textual / JSON form; for sql_variant the underlying
          base-type metadata is not preserved).
        - hierarchyid -> VARCHAR (the '/1/2/' path string).
        - geometry/geography -> GEOMETRY (the WKT form; a geography SRID such as 4326 is not preserved).
        - char/nchar wider than 2000 -> VARCHAR (Exasol CHAR max length = 2000). NOTE: Exasol CHAR comparison
          ignores trailing spaces (blank-padded) whereas VARCHAR does not.

      Length limit (Exasol VARCHAR max = 2,000,000 characters), controlled by TRUNCATE_LONG_STRINGS:
        - false (default): a value longer than 2,000,000 characters makes the IMPORT FAIL ('String data right
          truncation', ETL-3003) - nothing is truncated silently.
        - true: such values are CUT to 2,000,000 characters (LEFT(...)) and imported (conscious data loss).
      DECIMAL limit (Exasol max precision = 36), controlled by DECIMAL_OVERFLOW:
        - 'CAP' (default): DECIMAL(36,s); a value needing > 36 digits makes the IMPORT FAIL (ETL-3050).
        - 'DOUBLE': loads but keeps only ~15 significant digits.

    CHARACTER TYPES (varchar vs nvarchar and the "n..." types):
      SQL Server has non-Unicode types (char/varchar/text - one code page) and Unicode "n" types
      (nchar/nvarchar/ntext - UTF-16). ALL of them are migrated to Exasol UTF8 columns: UTF8 is lossless for
      any code page and is the safe universal target (Exasol's ASCII set is strictly 7-bit and would reject
      code-page data such as the default CP1252 collation). The byte-vs-character difference is handled:
      nchar/nvarchar report their length in BYTES (2 per char), so the Exasol length is the character count
      (nvarchar(50) -> VARCHAR(50), not VARCHAR(100)).

    BINARY TYPES (what they are / how migrated):
      binary(n)/varbinary(n)/image hold raw bytes (hashes, encrypted values, GUID bytes, small blobs);
      rowversion is an 8-byte auto-incrementing row-version counter. They are migrated faithfully as data
      (no longer dropped): see BINARY_HANDLING (default: fixed-length binary -> HASHTYPE, variable -> hex).

    NLS / COLLATION SAFETY:
      IMPORT FROM JDBC transfers TYPED values, so numbers, dates and timestamps are migrated by VALUE, not by
      their text form. Differing SQL Server or Exasol locale settings (decimal/thousands separators such as
      '.,' vs ',.', date formats, collations) therefore do NOT affect the migrated data.

    CONSTRAINTS (CONSTRAINT_STATE):
      Primary and foreign keys (composite keys included) are ALWAYS created DISABLED first, so the data load
      can never fail on key order or row order, and the IMPORTs are faster. A final "CONSTRAINT STATE"
      section (run AFTER the IMPORTs) then sets each key:
        - 'SET_AS_SOURCE'  : each key gets exactly its SQL Server state (enabled -> ENABLE, disabled -> DISABLE).
        - 'FORCE_DISABLE'  : every key is kept DISABLED (present as metadata only).
        - 'FORCE_ENABLE'   : every key is ENABLEd (validates the data in Exasol).
      Exasol is perfectly fine with DISABLED keys: as long as the key EXISTS (enabled or disabled), BI tools
      (Power BI, Tableau, ...) can generate better, faster SQL against the database, and disabled keys make
      the IMPORTs faster and order-independent. Use 'FORCE_ENABLE' only when you really need Exasol to
      re-validate the data. Recommended: 'FORCE_DISABLE' (or 'SET_AS_SOURCE' to mirror the source exactly).

    PARTITIONING (GENERATE_PARTITION_BY):
      SQL Server range partitioning (a partition function/scheme across filegroups) has no direct Exasol
      equivalent. With true, the partitioning COLUMN is added best-effort as a PARTITION BY clause inside the
      generated CREATE TABLE; with false it is omitted.

    Automatically excluded (always, no parameter - only real user data/structures are migrated): the built-in
    SYSTEM SCHEMAS (sys, INFORMATION_SCHEMA, guest and the fixed db_* role schemas), MICROSOFT-SHIPPED objects
    (is_ms_shipped = 1, e.g. sysdiagrams, dtproperties, spt_*, replication/CDC tables) and EXTERNAL / "virtual"
    tables (is_external = 1). The user's own schemas (incl. dbo) and objects are kept.

    Not migrated (out of scope): indexes (Exasol manages storage itself), UNIQUE/CHECK constraints (not
    supported by Exasol), functions/procedures/triggers, users/roles/permissions.

    Views: SQL Server view bodies are T-SQL and cannot be auto-translated; with GENERATE_VIEWS = true they
    are emitted as a COMMENTED "manual review" section.

    DATA VALIDATION (CHECK_MIGRATION): with true, the script additionally emits, for every migrated table, a
    "<table>_MIG_CHK" table of standardized cross-database-comparable metrics computed on BOTH systems (the
    Exasol target via a local SELECT and the SQL Server source via IMPORT) - row count, per-column NULL counts,
    numeric MIN/MAX/SUM (exact integer/decimal types only), date/datetime MIN/MAX (to the second) and DISTINCT
    counts - plus a DATABASE_MIGRATION."<schema>_MIG_CHK" summary flagging each metric OK / DEVIATION. Run this
    section AFTER the IMPORTs; review with SELECT * FROM DATABASE_MIGRATION."<schema>_MIG_CHK" WHERE "STATUS" =
    'DEVIATION'. The metric set is mapping-aware (e.g. float/real and binary/LOB/CLR/json/vector are excluded
    from value metrics) so faithful data yields zero deviations.

    Privileges: source metadata is read THROUGH the connection user, so only the objects that user can see
    are generated. Use a user with sufficient rights (e.g. db_owner / VIEW DEFINITION) to migrate everything.
*/
--/
create or replace script database_migration.SQLSERVER_TO_EXASOL(
  CONNECTION_NAME              -- name of the JDBC connection inside Exasol -> e.g. sqlserver_jdbc
  ,DB2SCHEMA                   -- true: SQL Server database.schema.table => Exasol "database"."schema_table"; false: schema.table => "schema"."table"
  ,DB_FILTER                   -- filter for the SQL Server database(s), e.g. 'master', 'ma%', 'first_db, second_db', '%'
  ,SCHEMA_FILTER               -- filter for the schemas, e.g. 'my_schema', 'my%', 'schema1, schema2', '%'
  ,TARGET_SCHEMA               -- target schema name on Exasol; empty string '' = use the source value (db or schema name)
  ,TABLE_FILTER                -- filter for the tables (and views), e.g. 'my_table', 'my%', 'table1, table2', '%'
  ,IDENTIFIER_CASE_INSENSITIVE -- true (recommended for SQL Server): fold ALL identifiers to UPPER so Exasol queries never need quotes; false: keep verbatim (quoted, preserves lower/MixedCase)
  ,CONSTRAINT_STATE            -- 'FORCE_DISABLE' (recommended; all keys kept DISABLED - metadata only), 'SET_AS_SOURCE' (each PK/FK ends in its SQL Server state) or 'FORCE_ENABLE' (all keys enabled). Keys are always created DISABLED; a final section run after the IMPORTs sets the state.
  ,GENERATE_COMMENTS           -- true/false: migrate MS_Description extended properties as COMMENT ON statements
  ,GENERATE_VIEWS              -- true/false: emit source view definitions as a commented manual-review section
  ,GENERATE_PARTITION_BY       -- true/false: add a best-effort PARTITION BY clause (from the SQL Server partitioning column) inside the CREATE TABLE
  ,BINARY_HANDLING             -- 'HASHTYPE' (fixed binary -> HASHTYPE, variable -> hex), 'HEX' (always hex VARCHAR) or 'SKIP' (load NULL)
  ,DECIMAL_OVERFLOW            -- 'CAP' (DECIMAL(36,s); IMPORT fails for values > 36 digits) or 'DOUBLE' (loads, ~15 significant digits) for source precision > 36
  ,TRUNCATE_LONG_STRINGS       -- true: values > 2,000,000 chars are CUT to 2,000,000 and imported; false: the IMPORT fails on such a value (no silent truncation)
  ,CHECK_MIGRATION             -- true/false: additionally emit data-validation metrics (per-table "<table>_MIG_CHK" + a "<schema>_MIG_CHK" summary comparing source vs target). Run AFTER the IMPORTs.
) RETURNS TABLE
AS

-- IDENTIFIER_CASE_INSENSITIVE = true wraps every identifier in upper(...) so it is stored UPPER CASE.
-- Applied CONSISTENTLY to schemas, tables, columns, primary keys, foreign keys, partition keys and comments.
exa_upper_begin=''
exa_upper_end=''
if IDENTIFIER_CASE_INSENSITIVE == true then
	exa_upper_begin='upper('
	exa_upper_end=')'
end

-- Normalize the option parameters; accept boolean true or the string 'TRUE' (any case) for the flags.
cstate = string.upper(tostring(CONSTRAINT_STATE))
if cstate ~= 'FORCE_ENABLE' and cstate ~= 'FORCE_DISABLE' then cstate = 'SET_AS_SOURCE' end
gen_comments = (GENERATE_COMMENTS == true) or (string.upper(tostring(GENERATE_COMMENTS)) == 'TRUE')
gen_views    = (GENERATE_VIEWS == true) or (string.upper(tostring(GENERATE_VIEWS)) == 'TRUE')
gen_part     = (GENERATE_PARTITION_BY == true) or (string.upper(tostring(GENERATE_PARTITION_BY)) == 'TRUE')
trunc        = (TRUNCATE_LONG_STRINGS == true) or (string.upper(tostring(TRUNCATE_LONG_STRINGS)) == 'TRUE')
binmode = string.upper(tostring(BINARY_HANDLING))
if binmode ~= 'HEX' and binmode ~= 'SKIP' then binmode = 'HASHTYPE' end
decof = string.upper(tostring(DECIMAL_OVERFLOW))
if decof ~= 'DOUBLE' then decof = 'CAP' end
gen_check = (CHECK_MIGRATION == true) or (string.upper(tostring(CHECK_MIGRATION)) == 'TRUE')
-- All character columns are mapped to UTF8: lossless for any source code page and the safe universal target.
csU = 'UTF8'

-- Build the IN / LIKE filter fragments. They are embedded inside a remote statement '...' string, so the
-- single quotes are doubled here.
if string.match(DB_FILTER, '%%') then
	DB_STR = [[like ('']]..DB_FILTER..[['')]]
else	DB_STR = [[in ('']]..DB_FILTER:gsub("^%s*(.-)%s*$", "%1"):gsub('%s*,%s*',"'',''")..[['')]]
end
if string.match(SCHEMA_FILTER, '%%') then
	SCHEMA_STR = [[like ('']]..SCHEMA_FILTER..[['')]]
else	SCHEMA_STR = [[in ('']]..SCHEMA_FILTER:gsub("^%s*(.-)%s*$", "%1"):gsub('%s*,%s*',"'',''")..[['')]]
end
if string.match(TABLE_FILTER, '%%') then
	TABLE_STR = [[like ('']]..TABLE_FILTER..[['')]]
else	TABLE_STR = [[in ('']]..TABLE_FILTER:gsub("^%s*(.-)%s*$", "%1"):gsub('%s*,%s*',"'',''")..[['')]]
end

-- Helper: wrap an identifier-producing SQL expression in upper(...) when case-insensitive.
function U(col) return exa_upper_begin..col..exa_upper_end end

-- Helper: build the Exasol target "schema"."table" expression for a row, honoring DB2SCHEMA and TARGET_SCHEMA.
function qname(schema_col, table_col, db_col)
	local sexpr, texpr
	if DB2SCHEMA then
		if TARGET_SCHEMA == null then sexpr = db_col else sexpr = [[']]..TARGET_SCHEMA..[[']] end
		texpr = schema_col..[[ || '_' || ]]..table_col
	else
		if TARGET_SCHEMA == null then sexpr = schema_col else sexpr = [[']]..TARGET_SCHEMA..[[']] end
		texpr = table_col
	end
	return [['"' || ]]..U(sexpr)..[[ || '"."' || ]]..U(texpr)..[[ || '"']]
end

-- Helper: build just the Exasol target schema name expression (for CREATE SCHEMA).
function sname(schema_col, db_col)
	local sexpr
	if DB2SCHEMA then
		if TARGET_SCHEMA == null then sexpr = db_col else sexpr = [[']]..TARGET_SCHEMA..[[']] end
	else
		if TARGET_SCHEMA == null then sexpr = schema_col else sexpr = [[']]..TARGET_SCHEMA..[[']] end
	end
	return U(sexpr)
end

-- Get the list of source databases matching DB_FILTER.
success1, res1 = pquery([[
	select * from (import from jdbc at ]]..CONNECTION_NAME..[[ statement ' select name from sys.databases where name ]]..DB_STR..[[ ')
]],{})
if not success1 then error('Error getting database list from SQL Server: '..res1.error_message) end
if (#res1) < 1 then error('No database found for DB_FILTER.') end

-- Helper: turn a per-database remote-query template (using the @DB@ placeholder) into a UNION ALL across all
-- matched databases. SQL Server catalog views are per database, so each database is queried in turn.
function per_db(tmpl)
	local parts = {}
	for i = 1, (#res1) do parts[i] = (tmpl:gsub('@DB@', res1[i][1])) end
	return table.concat(parts, [[
 UNION ALL ]])
end

-------------------------------------------------------------------------------------------------------
-- Remote (SQL Server side) metadata queries. Literals are quote-doubled because they are embedded inside a
-- remote statement '...' string. BASE_TYPE_NAME resolves alias types to their base; IS_CLR_UDT flags CLR types.
-------------------------------------------------------------------------------------------------------
-- Fixed exclusion of non-user objects (always on, no parameter): the built-in SYSTEM SCHEMAS, Microsoft-
-- shipped objects (e.g. sysdiagrams, dtproperties, spt_*, replication/CDC tables - is_ms_shipped = 1) and
-- external / "virtual" tables (is_external = 1). The user's own data in dbo and other schemas is kept. The
-- list and the is_ms_shipped / is_external columns are identical across SQL Server 2016-2025 and Azure SQL.
sys_schemas = [[''sys'',''INFORMATION_SCHEMA'',''guest'',''db_owner'',''db_accessadmin'',''db_securityadmin'',''db_ddladmin'',''db_backupoperator'',''db_datareader'',''db_datawriter'',''db_denydatareader'',''db_denydatawriter'']]
columns_tmpl = [[select ''@DB@'' as DB_NAME, s.name as SCHEMA_NAME, t.name as TABLE_NAME, c.column_id as COLUMN_ID, c.name as COLUMN_NAME, case when ty.is_user_defined = 0 then ty.name else type_name(ty.system_type_id) end as BASE_TYPE_NAME, ty.name as TYPE_NAME, cast(ty.is_assembly_type as int) as IS_CLR_UDT, cast(ty.is_user_defined as int) as IS_USER_DEF, cast(c.max_length as int) as COL_MAX_LENGTH, cast(c.precision as int) as PRECISION, cast(c.scale as int) as SCALE, cast(c.is_nullable as int) as IS_NULLABLE, cast(c.is_identity as int) as IS_IDENTITY, pk.pk_name as PK_NAME, pk.key_ordinal as PK_ORDINAL, cast(pk.pk_disabled as int) as PK_DISABLED, dc.definition as DEFAULT_DEF from [@DB@].sys.schemas s join [@DB@].sys.tables t on s.schema_id = t.schema_id join [@DB@].sys.columns c on c.object_id = t.object_id join [@DB@].sys.types ty on c.user_type_id = ty.user_type_id left join [@DB@].sys.default_constraints dc on dc.object_id = c.default_object_id left join (select ic.object_id, ic.column_id, ic.key_ordinal, kc.name as pk_name, i.is_disabled as pk_disabled from [@DB@].sys.key_constraints kc join [@DB@].sys.indexes i on i.object_id = kc.parent_object_id and i.index_id = kc.unique_index_id join [@DB@].sys.index_columns ic on ic.object_id = kc.parent_object_id and ic.index_id = kc.unique_index_id where kc.type = ''PK'') pk on pk.object_id = t.object_id and pk.column_id = c.column_id where s.name ]]..SCHEMA_STR..[[ and t.name ]]..TABLE_STR..[[ and s.name not in (]]..sys_schemas..[[) and t.is_ms_shipped = 0 and t.is_external = 0]]

fk_tmpl = [[select ''@DB@'' as DB_NAME, fk.name as FK_NAME, cast(fk.is_disabled as int) as FK_DISABLED, sp.name as PARENT_SCHEMA, tp.name as PARENT_TABLE, cp.name as PARENT_COL, sr.name as REF_SCHEMA, tr.name as REF_TABLE, cr.name as REF_COL, fkc.constraint_column_id as ORD from [@DB@].sys.foreign_keys fk join [@DB@].sys.foreign_key_columns fkc on fkc.constraint_object_id = fk.object_id join [@DB@].sys.tables tp on fk.parent_object_id = tp.object_id join [@DB@].sys.schemas sp on tp.schema_id = sp.schema_id join [@DB@].sys.columns cp on cp.object_id = tp.object_id and cp.column_id = fkc.parent_column_id join [@DB@].sys.tables tr on fk.referenced_object_id = tr.object_id join [@DB@].sys.schemas sr on tr.schema_id = sr.schema_id join [@DB@].sys.columns cr on cr.object_id = tr.object_id and cr.column_id = fkc.referenced_column_id where sp.name ]]..SCHEMA_STR..[[ and tp.name ]]..TABLE_STR..[[ and sp.name not in (]]..sys_schemas..[[) and tp.is_ms_shipped = 0 and tp.is_external = 0]]

part_tmpl = [[select distinct ''@DB@'' as DB_NAME, s.name as SCHEMA_NAME, t.name as TABLE_NAME, col.name as COL_NAME, ic.partition_ordinal as ORD from [@DB@].sys.tables t join [@DB@].sys.schemas s on t.schema_id = s.schema_id join [@DB@].sys.indexes i on i.object_id = t.object_id and i.index_id <= 1 join [@DB@].sys.index_columns ic on ic.object_id = i.object_id and ic.index_id = i.index_id and ic.partition_ordinal > 0 join [@DB@].sys.columns col on col.object_id = t.object_id and col.column_id = ic.column_id where s.name ]]..SCHEMA_STR..[[ and t.name ]]..TABLE_STR..[[ and s.name not in (]]..sys_schemas..[[) and t.is_ms_shipped = 0 and t.is_external = 0]]

comments_tmpl = [[select ''@DB@'' as DB_NAME, s.name as SCHEMA_NAME, t.name as TABLE_NAME, isnull(c.name, '''') as COLUMN_NAME, cast(ep.minor_id as int) as MINOR_ID, cast(ep.value as nvarchar(2000)) as DESCR from [@DB@].sys.extended_properties ep join [@DB@].sys.tables t on ep.major_id = t.object_id join [@DB@].sys.schemas s on t.schema_id = s.schema_id left join [@DB@].sys.columns c on c.object_id = t.object_id and c.column_id = ep.minor_id where ep.class = 1 and ep.name = ''MS_Description'' and s.name ]]..SCHEMA_STR..[[ and t.name ]]..TABLE_STR..[[ and s.name not in (]]..sys_schemas..[[) and t.is_ms_shipped = 0 and t.is_external = 0]]

views_tmpl = [[select ''@DB@'' as DB_NAME, s.name as SCHEMA_NAME, v.name as VIEW_NAME, m.definition as DEF from [@DB@].sys.views v join [@DB@].sys.schemas s on v.schema_id = s.schema_id join [@DB@].sys.sql_modules m on m.object_id = v.object_id where m.definition is not null and s.name ]]..SCHEMA_STR..[[ and v.name ]]..TABLE_STR..[[ and s.name not in (]]..sys_schemas..[[) and v.is_ms_shipped = 0]]

qcols  = per_db(columns_tmpl)
qfk    = per_db(fk_tmpl)
qpart  = per_db(part_tmpl)
qcomm  = per_db(comments_tmpl)
qviews = per_db(views_tmpl)

-------------------------------------------------------------------------------------------------------
-- Exasol side expressions (evaluated on the imported metadata) - they produce the generated statement text.
-------------------------------------------------------------------------------------------------------
-- Supported-type predicate: a column is unsupported if it is a CLR/assembly UDT, or its base type is unknown.
known_types = [['bit','tinyint','smallint','int','bigint','decimal','numeric','money','smallmoney','float','real','char','varchar','text','nchar','nvarchar','ntext','sysname','uniqueidentifier','xml','json','vector','date','datetime','smalldatetime','datetime2','time','datetimeoffset','binary','varbinary','image','timestamp','rowversion','sql_variant','hierarchyid','geometry','geography']]
-- A user CLR/assembly UDT (is_user_defined AND is_assembly_type) cannot be migrated; neither can an unknown
-- base type. Built-in CLR types (geometry/geography/hierarchyid, is_user_defined=0) ARE supported.
unsup = [[((IS_USER_DEF = 1 and IS_CLR_UDT = 1) or BASE_TYPE_NAME is null or lower(BASE_TYPE_NAME) not in (]]..known_types..[[))]]

-- DECIMAL precision > 36 (Exasol max): cap to DECIMAL(36,s) or fall back to DOUBLE.
if decof == 'DOUBLE' then
	dec_expr = [[case when PRECISION > 36 then 'DOUBLE' else 'DECIMAL(' || PRECISION || ',' || SCALE || ')' end]]
else
	dec_expr = [[case when PRECISION > 36 then 'DECIMAL(36,' || (case when SCALE > 36 then 36 else SCALE end) || ')' else 'DECIMAL(' || PRECISION || ',' || SCALE || ')' end]]
end

-- Binary target types (depend on BINARY_HANDLING).
if binmode == 'HASHTYPE' then
	bin_fixed = [[case when COL_MAX_LENGTH <= 1024 then 'HASHTYPE(' || COL_MAX_LENGTH || ' BYTE)' else 'VARCHAR(' || (case when COL_MAX_LENGTH*2 > 2000000 then 2000000 else COL_MAX_LENGTH*2 end) || ') ASCII' end]]
	rv_type   = [['HASHTYPE(8 BYTE)']]
elseif binmode == 'HEX' then
	bin_fixed = [['VARCHAR(' || (case when COL_MAX_LENGTH*2 > 2000000 then 2000000 else COL_MAX_LENGTH*2 end) || ') ASCII']]
	rv_type   = [['VARCHAR(16) ASCII']]
else  -- SKIP
	bin_fixed = [['VARCHAR(2000000) UTF8']]
	rv_type   = [['VARCHAR(2000000) UTF8']]
end
if binmode == 'SKIP' then
	bin_var = [['VARCHAR(2000000) UTF8']]
else
	bin_var = [['VARCHAR(' || (case when COL_MAX_LENGTH < 1 or COL_MAX_LENGTH*2 > 2000000 then 2000000 else COL_MAX_LENGTH*2 end) || ') ASCII']]
end

-- Exasol column type expression (CREATE TABLE), switched on the base type name.
col_type_expr = [[case BASE_TYPE_NAME
	when 'bit' then 'DECIMAL(1,0)'
	when 'tinyint' then 'DECIMAL(3,0)'
	when 'smallint' then 'DECIMAL(5,0)'
	when 'int' then 'DECIMAL(10,0)'
	when 'bigint' then 'DECIMAL(19,0)'
	when 'decimal' then ]]..dec_expr..[[
	when 'numeric' then ]]..dec_expr..[[
	when 'money' then 'DECIMAL(19,4)'
	when 'smallmoney' then 'DECIMAL(10,4)'
	when 'float' then 'DOUBLE'
	when 'real' then 'DOUBLE'
	when 'char' then case when COL_MAX_LENGTH > 2000 then 'VARCHAR(' || COL_MAX_LENGTH || ') ]]..csU..[[' else 'CHAR(' || COL_MAX_LENGTH || ') ]]..csU..[[' end
	when 'varchar' then 'VARCHAR(' || (case when COL_MAX_LENGTH < 1 then 2000000 else COL_MAX_LENGTH end) || ') ]]..csU..[['
	when 'text' then 'VARCHAR(2000000) ]]..csU..[['
	when 'nchar' then case when floor(COL_MAX_LENGTH/2) > 2000 then 'VARCHAR(' || floor(COL_MAX_LENGTH/2) || ') ]]..csU..[[' else 'CHAR(' || floor(COL_MAX_LENGTH/2) || ') ]]..csU..[[' end
	when 'nvarchar' then 'VARCHAR(' || (case when COL_MAX_LENGTH < 1 then 2000000 else floor(COL_MAX_LENGTH/2) end) || ') ]]..csU..[['
	when 'ntext' then 'VARCHAR(2000000) ]]..csU..[['
	when 'sysname' then 'VARCHAR(' || (case when COL_MAX_LENGTH < 1 then 128 else floor(COL_MAX_LENGTH/2) end) || ') ]]..csU..[['
	when 'uniqueidentifier' then 'CHAR(36) ASCII'
	when 'xml' then 'VARCHAR(2000000) ]]..csU..[['
	when 'json' then 'VARCHAR(2000000) ]]..csU..[['
	when 'vector' then 'VARCHAR(2000000) ]]..csU..[['
	when 'date' then 'DATE'
	when 'datetime' then 'TIMESTAMP(3)'
	when 'smalldatetime' then 'TIMESTAMP(0)'
	when 'datetime2' then 'TIMESTAMP(' || (case when SCALE > 9 then 9 else SCALE end) || ')'
	when 'time' then 'VARCHAR(16) ASCII'
	when 'datetimeoffset' then 'TIMESTAMP(' || (case when SCALE > 9 then 9 else SCALE end) || ') WITH LOCAL TIME ZONE'
	when 'binary' then ]]..bin_fixed..[[
	when 'varbinary' then ]]..bin_var..[[
	when 'image' then ]]..bin_var..[[
	when 'timestamp' then ]]..rv_type..[[
	when 'rowversion' then ]]..rv_type..[[
	when 'sql_variant' then 'VARCHAR(2000000) ]]..csU..[['
	when 'hierarchyid' then 'VARCHAR(4000) ]]..csU..[['
	when 'geometry' then 'GEOMETRY'
	when 'geography' then 'GEOMETRY'
	else 'VARCHAR(2000000) ]]..csU..[['
end]]

-- Source SELECT expressions. Quotes that must survive into the generated remote statement are doubled.
-- datetime2/datetimeoffset transfer TYPED (full fractional precision); binary/rowversion convert to hex.
if binmode == 'SKIP' then
	bin_imp = [['NULL']]
	rv_imp  = [['NULL']]
else
	bin_imp = [['CONVERT(VARCHAR(MAX), CAST([' || COLUMN_NAME || '] AS VARBINARY(MAX)), 2)']]
	rv_imp  = [['CONVERT(VARCHAR(16), CAST([' || COLUMN_NAME || '] AS VARBINARY(8)), 2)']]
end
-- Long character/LOB columns: optionally cut to 2,000,000 chars (TRUNCATE_LONG_STRINGS = true).
if trunc then
	xml_imp = [['LEFT(CAST([' || COLUMN_NAME || '] AS NVARCHAR(MAX)), 2000000)']]
	txt_imp = [['LEFT(CAST([' || COLUMN_NAME || '] AS NVARCHAR(MAX)), 2000000)']]
else
	xml_imp = [['CAST([' || COLUMN_NAME || '] AS NVARCHAR(MAX))']]
	txt_imp = [['[' || COLUMN_NAME || ']']]
end

src_expr = [[case BASE_TYPE_NAME
	when 'varchar' then ]]..txt_imp..[[
	when 'nvarchar' then ]]..txt_imp..[[
	when 'text' then ]]..txt_imp..[[
	when 'ntext' then ]]..txt_imp..[[
	when 'xml' then ]]..xml_imp..[[
	when 'json' then ]]..xml_imp..[[
	when 'vector' then ]]..xml_imp..[[
	when 'sql_variant' then ]]..xml_imp..[[
	when 'time' then 'CAST([' || COLUMN_NAME || '] AS VARCHAR(16))'
	when 'datetimeoffset' then 'CAST([' || COLUMN_NAME || '] AT TIME ZONE ''''UTC'''' AS datetime2(' || (case when SCALE > 7 then 7 else SCALE end) || '))'
	when 'hierarchyid' then '[' || COLUMN_NAME || '].ToString()'
	when 'geometry' then '[' || COLUMN_NAME || '].STAsText()'
	when 'geography' then '[' || COLUMN_NAME || '].STAsText()'
	when 'binary' then ]]..bin_imp..[[
	when 'varbinary' then ]]..bin_imp..[[
	when 'image' then ]]..bin_imp..[[
	when 'timestamp' then ]]..rv_imp..[[
	when 'rowversion' then ]]..rv_imp..[[
	else '[' || COLUMN_NAME || ']'
end]]

-- Conservative DEFAULT mapping: numeric/string literals and the common "now" functions; anything else
-- (e.g. newid()) is left out so it can never produce an invalid CREATE TABLE.
default_expr = [[case
	when DEFAULT_DEF is null then ''
	when DEFAULT_DEF REGEXP_LIKE '^\(+\s*[-]{0,1}[0-9]+(\.[0-9]+){0,1}\s*\)+$' then ' DEFAULT ' || regexp_replace(DEFAULT_DEF, '[()]', '')
	when DEFAULT_DEF REGEXP_LIKE '^\(\s*[N]{0,1}''.*''\s*\)$' then ' DEFAULT ' || regexp_replace(regexp_replace(DEFAULT_DEF, '^\(\s*[N]{0,1}', ''), '\s*\)$', '')
	when upper(regexp_replace(DEFAULT_DEF, '[() ]', '')) in ('GETDATE','SYSDATETIME','CURRENT_TIMESTAMP','GETUTCDATE','SYSUTCDATETIME') then ' DEFAULT CURRENT_TIMESTAMP'
	else ''
end]]

-- Constraint-state ALTER suffix (the final CONSTRAINT STATE section), with an explanatory comment.
if cstate == 'FORCE_ENABLE' then
	pk_state = [[' enable;  -- forced ENABLE (validates data in Exasol; source state ignored)']]
	fk_state = [[' enable;  -- forced ENABLE (validates data in Exasol; source state ignored)']]
elseif cstate == 'FORCE_DISABLE' then
	pk_state = [[' disable;  -- forced DISABLE (kept as optimizer/BI metadata only; source state ignored)']]
	fk_state = [[' disable;  -- forced DISABLE (kept as optimizer/BI metadata only; source state ignored)']]
else
	pk_state = [[case when PK_DISABLED = 1 then ' disable;  -- matches SQL Server source (was DISABLED)' else ' enable;  -- matches SQL Server source (was ENABLED)' end]]
	fk_state = [[case when FK_DISABLED = 1 then ' disable;  -- matches SQL Server source (was DISABLED)' else ' enable;  -- matches SQL Server source (was ENABLED)' end]]
end

-- Precompute the qualified-name expressions for each context.
main_qname = qname('SCHEMA_NAME', 'TABLE_NAME', 'DB_NAME')
main_sname = sname('SCHEMA_NAME', 'DB_NAME')
fk_parent  = qname('PARENT_SCHEMA', 'PARENT_TABLE', 'DB_NAME')
fk_ref     = qname('REF_SCHEMA', 'REF_TABLE', 'DB_NAME')

-------------------------------------------------------------------------------------------------------
-- Optional CTEs / output rows (partition, comments, views), assembled only when requested.
-------------------------------------------------------------------------------------------------------
-- PARTITION BY is emitted INSIDE the CREATE TABLE (Exasol syntax: ", PARTITION BY col" after the columns,
-- no parentheses). vv_part yields the clause + a trailing comment, joined into cr_tables below.
part_cte = ''
part_join = ''
part_clause_sel = ''
part_note_sel = ''
if gen_part then
	part_cte = [[
,cr_part_raw as (select * from (import from jdbc at ]]..CONNECTION_NAME..[[ statement ']]..qpart..[['))
,vv_part as (
	select DB_NAME, SCHEMA_NAME, TABLE_NAME, ', PARTITION BY ' || group_concat('"' || ]]..U('COL_NAME')..[[ || '"' order by ORD) as part_clause, '  -- PARTITION BY is best-effort from the SQL Server partitioning column (range boundaries/filegroups are not applicable in Exasol)' as part_note
	from cr_part_raw group by DB_NAME, SCHEMA_NAME, TABLE_NAME
)]]
	part_join = [[
	left join vv_part vp on vp.DB_NAME = t.DB_NAME and vp.SCHEMA_NAME = t.SCHEMA_NAME and vp.TABLE_NAME = t.TABLE_NAME]]
	part_clause_sel = [[ || coalesce(vp.part_clause, '')]]
	part_note_sel = [[ || coalesce(vp.part_note, '')]]
end

comments_cte = ''
comments_union = ''
if gen_comments then
	comments_cte = [[
,cr_comments_raw as (select * from (import from jdbc at ]]..CONNECTION_NAME..[[ statement ']]..qcomm..[['))
,vv_comments as (
	select case when MINOR_ID = 0
		then 'comment on table ' || ]]..qname('SCHEMA_NAME','TABLE_NAME','DB_NAME')..[[ || ' is ' || '''' || replace(DESCR, '''', '''''') || '''' || ';'
		else 'comment on column ' || ]]..qname('SCHEMA_NAME','TABLE_NAME','DB_NAME')..[[ || '."' || ]]..U('COLUMN_NAME')..[[ || '"' || ' is ' || '''' || replace(DESCR, '''', '''''') || '''' || ';'
	end as sql_text
	from cr_comments_raw where DESCR is not null
)]]
	comments_union = "\n"..[[UNION ALL select 41, cast('-- ### COMMENTS ###' as varchar(2000000)) SQL_TEXT
UNION ALL select 42, sql_text from vv_comments]]
end

views_cte = ''
views_union = ''
if gen_views then
	views_cte = [[
,cr_views_raw as (select * from (import from jdbc at ]]..CONNECTION_NAME..[[ statement ']]..qviews..[['))
,vv_views as (
	select '-- ' || DB_NAME || '.' || SCHEMA_NAME || '.' || VIEW_NAME || '  (T-SQL source view - review and adapt to Exasol SQL manually):' || chr(10) || '-- ' || replace(DEF, chr(10), chr(10) || '-- ') as sql_text
	from cr_views_raw
)]]
	views_union = "\n"..[[UNION ALL select 90, cast('-- ### VIEWS (T-SQL - commented out, manual review required) ###' as varchar(2000000)) SQL_TEXT
UNION ALL select 91, sql_text from vv_views]]
end

-- CHECK_MIGRATION: per migrated table a wide single-scan metrics row on BOTH systems (Exasol target via a local
-- SELECT + SQL Server source via IMPORT) into "<table>_MIG_CHK"; a per-schema DATABASE_MIGRATION."<schema>_MIG_CHK"
-- summary unpivots+joins them, flagging each metric OK/DEVIATION. Mapping-aware: only cross-comparable metrics per
-- type (row/NULL/DISTINCT counts; numeric MIN/MAX/SUM on EXACT integer/decimal types only - not float/real;
-- date/datetime MIN/MAX as text to the second). The 'SQLServer' label is added on the Exasol side so the source
-- SELECT carries no string literals (no extra quote-nesting).
check_cte = ''
check_union = ''
if gen_check then
	c_num    = [[BASE_TYPE_NAME in ('tinyint','smallint','int','bigint','decimal','numeric','money','smallmoney')]]
	c_dt     = [[BASE_TYPE_NAME in ('date','datetime','datetime2','smalldatetime')]]
	c_dist   = [[BASE_TYPE_NAME in ('bit','tinyint','smallint','int','bigint','decimal','numeric','money','smallmoney','date','datetime','datetime2','smalldatetime','uniqueidentifier')]]
	exa_dtf  = [[(case when BASE_TYPE_NAME = 'date' then '''YYYY-MM-DD''' else '''YYYY-MM-DD HH24:MI:SS''' end)]]
	ss_len   = [[(case when BASE_TYPE_NAME = 'date' then 10 else 19 end)]]
	ss_sty   = [[(case when BASE_TYPE_NAME = 'date' then 23 else 120 end)]]
	if DB2SCHEMA then chk_tbl = U([[SCHEMA_NAME || '_' || TABLE_NAME || '_MIG_CHK']]) else chk_tbl = U([[TABLE_NAME || '_MIG_CHK']]) end
	check_cte = [[
,vv_chk_cols as (
	select DB_NAME, SCHEMA_NAME, TABLE_NAME, COLUMN_ID, COLUMN_NAME, BASE_TYPE_NAME, IS_NULLABLE,
		(case when BASE_TYPE_NAME in ('money','smallmoney') then 4 when SCALE < 0 then 0 when SCALE > 6 then 6 else SCALE end) as MSC,
		'"' || ]]..U('COLUMN_NAME')..[[ || '"' as EREF, '[' || COLUMN_NAME || ']' as SREF,
		min(COLUMN_ID) over (partition by DB_NAME, SCHEMA_NAME, TABLE_NAME) as MIN_COLID,
		]]..main_qname..[[ as TGT, ]]..main_sname..[[ as TGTSCH, '"' || ]]..main_sname..[[ || '"."' || ]]..chk_tbl..[[ || '"' as WIDEQ
	from sqlserv_base where not (]]..unsup..[[)
)
,vv_chk_x as (
	select c.*, sysrow.DB_SYSTEM, m.metric_id,
		(case when sysrow.DB_SYSTEM = 'Exasol' then c.EREF else c.SREF end) as CREF,
		(case when sysrow.DB_SYSTEM = 'Exasol' then 'count' else 'count_big' end) as CFN
	from vv_chk_cols c
	cross join (select 'Exasol' as DB_SYSTEM union all select 'SQLServer' as DB_SYSTEM) sysrow
	cross join (select level-1 as metric_id from dual connect by level <= 6) m
)
,vv_chk_e as (
	select DB_NAME, SCHEMA_NAME, TABLE_NAME, COLUMN_ID, COLUMN_NAME, TGT, TGTSCH, WIDEQ, DB_SYSTEM, metric_id,
		(case metric_id
			when 0 then case when COLUMN_ID = MIN_COLID then 'cast(' || CFN || '(*) as decimal(36,0))' end
			when 1 then case when IS_NULLABLE = 1 then 'cast(' || CFN || '(case when ' || CREF || ' is null then 1 end) as decimal(36,0))' end
			when 2 then (case when ]]..c_num..[[ then 'cast(min(' || CREF || ') as decimal(36,' || MSC || '))'
			                  when ]]..c_dt..[[ then (case when DB_SYSTEM = 'Exasol' then 'to_char(min(' || CREF || '),' || ]]..exa_dtf..[[ || ')' else 'convert(varchar(' || ]]..ss_len..[[ || '),min(' || CREF || '),' || ]]..ss_sty..[[ || ')' end) end)
			when 3 then (case when ]]..c_num..[[ then 'cast(max(' || CREF || ') as decimal(36,' || MSC || '))'
			                  when ]]..c_dt..[[ then (case when DB_SYSTEM = 'Exasol' then 'to_char(max(' || CREF || '),' || ]]..exa_dtf..[[ || ')' else 'convert(varchar(' || ]]..ss_len..[[ || '),max(' || CREF || '),' || ]]..ss_sty..[[ || ')' end) end)
			when 4 then case when ]]..c_num..[[ then 'cast(sum(' || CREF || ') as decimal(36,' || MSC || '))' end
			when 5 then case when ]]..c_dist..[[ then 'cast(' || CFN || '(distinct ' || CREF || ') as decimal(36,0))' end
		end) as MEXPR,
		(case metric_id when 0 then 'ROW_CNT' when 1 then COLUMN_NAME || '_NULLS' when 2 then COLUMN_NAME || '_MIN' when 3 then COLUMN_NAME || '_MAX' when 4 then COLUMN_NAME || '_SUM' when 5 then COLUMN_NAME || '_DISTINCT' end) as MNAME
	from vv_chk_x
)
,vv_chk_named as (select * from vv_chk_e where MEXPR is not null)
,vv_chk_sys as (
	select DB_NAME, SCHEMA_NAME, TABLE_NAME, TGTSCH, WIDEQ, DB_SYSTEM,
		case when DB_SYSTEM = 'Exasol'
			then 'select ''Exasol'' as "DB_SYSTEM", ' || group_concat(MEXPR || ' as "' || ]]..U('MNAME')..[[ || '"' order by COLUMN_ID, metric_id separator ', ') || ' from ' || max(TGT)
			else 'select ''SQLServer'' as "DB_SYSTEM", x.* from (import from jdbc at ]]..CONNECTION_NAME..[[ statement ' || '''' || 'select ' || group_concat(MEXPR order by COLUMN_ID, metric_id separator ', ') || ' from [' || max(DB_NAME) || '].[' || max(SCHEMA_NAME) || '].[' || max(TABLE_NAME) || ']' || '''' || ') x'
		end as SEL
	from vv_chk_named group by DB_NAME, SCHEMA_NAME, TABLE_NAME, TGTSCH, WIDEQ, DB_SYSTEM
)
,vv_chk_wide as (
	select 'create or replace table ' || WIDEQ || ' as ' || max(case when DB_SYSTEM = 'Exasol' then SEL end) || ' UNION ALL ' || max(case when DB_SYSTEM = 'SQLServer' then SEL end) || ';' as sql_text
	from vv_chk_sys group by TGTSCH, WIDEQ
)
,vv_chk_unpiv as (
	select TGTSCH, TABLE_NAME, COLUMN_ID, metric_id, DB_SYSTEM, WIDEQ, MNAME,
		'select ' || '''' || TABLE_NAME || '''' || ' as "TABLE_NAME", ' || '''' || MNAME || '''' || ' as "METRIC", to_char("' || ]]..U('MNAME')..[[ || '") as "VAL" from ' || WIDEQ || ' where "DB_SYSTEM" = ' || '''' || DB_SYSTEM || '''' as FRAG
	from vv_chk_named
)
,vv_chk_summary as (
	select 'create or replace table "DATABASE_MIGRATION"."' || ]]..U('TGTSCH')..[[ || '_MIG_CHK" as select e."TABLE_NAME", e."METRIC", e."VAL" as "EXASOL_METRIC", s."VAL" as "SQLSERVER_METRIC", case when coalesce(e."VAL", ''~NULL~'') = coalesce(s."VAL", ''~NULL~'') then ''OK'' else ''DEVIATION'' end as "STATUS" from (' || group_concat(case when DB_SYSTEM = 'Exasol' then FRAG end order by TABLE_NAME, COLUMN_ID, metric_id separator ' union all ') || ') e join (' || group_concat(case when DB_SYSTEM = 'SQLServer' then FRAG end order by TABLE_NAME, COLUMN_ID, metric_id separator ' union all ') || ') s on e."TABLE_NAME" = s."TABLE_NAME" and e."METRIC" = s."METRIC" order by "STATUS" desc, e."TABLE_NAME", e."METRIC";' as sql_text
	from vv_chk_unpiv group by TGTSCH
)]]
	check_union = "\n"..[[UNION ALL select 70, cast('-- ### DATA VALIDATION (CHECK_MIGRATION) - run AFTER the IMPORTs; compares source vs target metrics ###' as varchar(2000000)) SQL_TEXT
UNION ALL select 71, sql_text from vv_chk_wide
UNION ALL select 72, cast('-- per-schema validation summary (one row per metric; STATUS = OK / DEVIATION):' as varchar(2000000))
UNION ALL select 73, sql_text from vv_chk_summary
UNION ALL select 74, cast('-- review deviations with:  SELECT * FROM DATABASE_MIGRATION."<schema>_MIG_CHK" WHERE "STATUS" = ''DEVIATION'';' as varchar(2000000))]]
end

-------------------------------------------------------------------------------------------------------
-- Main query: build all sections and return them in execution order.
-------------------------------------------------------------------------------------------------------
suc, res = pquery([[
with sqlserv_base as (
	select * from (import from jdbc at ]]..CONNECTION_NAME..[[ statement ']]..qcols..[[')
)
,vv_unsupported as (
	select '-- !!! UNSUPPORTED TYPE - column NOT migrated: ' || ]]..qname('SCHEMA_NAME','TABLE_NAME','DB_NAME')..[[ || '."' || COLUMN_NAME || '" (SQL Server type: ' || coalesce(TYPE_NAME, 'unknown') || ') - migrate this column manually !!!' as sql_text
	from sqlserv_base where ]]..unsup..[[
)
,vv_pk as (
	select DB_NAME, SCHEMA_NAME, TABLE_NAME, PK_NAME, max(PK_DISABLED) as PK_DISABLED,
		', constraint "' || ]]..U('PK_NAME')..[[ || '" primary key (' || group_concat('"' || ]]..U('COLUMN_NAME')..[[ || '"' order by PK_ORDINAL) || ') DISABLE' as pk_con
	from sqlserv_base where PK_ORDINAL is not null
	group by DB_NAME, SCHEMA_NAME, TABLE_NAME, PK_NAME
)
,cr_fk_raw as (
	select * from (import from jdbc at ]]..CONNECTION_NAME..[[ statement ']]..qfk..[[')
)
,vv_fk as (
	select DB_NAME, PARENT_SCHEMA, PARENT_TABLE, FK_NAME, max(FK_DISABLED) as FK_DISABLED,
		'alter table ' || ]]..fk_parent..[[ || ' add constraint "' || ]]..U('FK_NAME')..[[ || '" foreign key (' || group_concat('"' || ]]..U('PARENT_COL')..[[ || '"' order by ORD) || ') references ' || ]]..fk_ref..[[ || ' (' || group_concat('"' || ]]..U('REF_COL')..[[ || '"' order by ORD) || ') DISABLE;' as sql_text
	from cr_fk_raw group by DB_NAME, FK_NAME, PARENT_SCHEMA, PARENT_TABLE, REF_SCHEMA, REF_TABLE
)]]..part_cte..[[
,cr_schemas as (
	select distinct 'create schema if not exists "' || ]]..main_sname..[[ || '";' as sql_text from sqlserv_base
)
,cr_tables as (
	select 'create or replace table ' || tname || ' (' || cols || coalesce(pk_con, '')]]..part_clause_sel..[[ || ');']]..part_note_sel..[[ as sql_text
	from (
		select DB_NAME, SCHEMA_NAME, TABLE_NAME, ]]..main_qname..[[ as tname,
			group_concat(case when ]]..unsup..[[ then NULL else '"' || ]]..U('COLUMN_NAME')..[[ || '" ' || (]]..col_type_expr..[[) || (case when IS_IDENTITY = 1 then ' IDENTITY' else '' end) || (]]..default_expr..[[) || (case when IS_NULLABLE = 0 then ' NOT NULL' else '' end) end order by COLUMN_ID separator ', ') as cols
		from sqlserv_base group by DB_NAME, SCHEMA_NAME, TABLE_NAME
	) t
	left join vv_pk v on v.DB_NAME = t.DB_NAME and v.SCHEMA_NAME = t.SCHEMA_NAME and v.TABLE_NAME = t.TABLE_NAME]]..part_join..[[
)
,cr_imports as (
	select 'import into ' || ]]..main_qname..[[ || ' (' || group_concat(case when ]]..unsup..[[ then NULL else '"' || ]]..U('COLUMN_NAME')..[[ || '"' end order by COLUMN_ID separator ', ') || ') from jdbc at ]]..CONNECTION_NAME..[[ statement ' || '''' || 'select ' || group_concat(case when ]]..unsup..[[ then NULL else (]]..src_expr..[[) end order by COLUMN_ID separator ', ') || ' from [' || DB_NAME || '].[' || SCHEMA_NAME || '].[' || TABLE_NAME || ']' || '''' || ';' as sql_text
	from sqlserv_base group by DB_NAME, SCHEMA_NAME, TABLE_NAME
)]]..comments_cte..views_cte..check_cte..[[
select SQL_TEXT from (
	select 0 as ord, sql_text SQL_TEXT from vv_unsupported
	UNION ALL select 1, cast('-- ### SCHEMAS ###' as varchar(2000000)) SQL_TEXT
	UNION ALL select 2, sql_text from cr_schemas
	UNION ALL select 3, cast('-- ### TABLES (incl. PRIMARY KEY, created DISABLED) ###' as varchar(2000000)) SQL_TEXT
	UNION ALL select 4, sql_text from cr_tables where sql_text not like '%();%'
	UNION ALL select 5, cast('-- ### FOREIGN KEYS (created DISABLED) ###' as varchar(2000000)) SQL_TEXT
	UNION ALL select 6, sql_text from vv_fk]]..comments_union..[[
	UNION ALL select 50, cast('-- ### IMPORTS ###' as varchar(2000000)) SQL_TEXT
	UNION ALL select 51, sql_text from cr_imports
	UNION ALL select 60, cast('-- ### CONSTRAINT STATE - run AFTER the data load (keys were created DISABLED for a fast, order-independent load) ###' as varchar(2000000)) SQL_TEXT
	UNION ALL select 61, 'alter table ' || ]]..qname('SCHEMA_NAME','TABLE_NAME','DB_NAME')..[[ || ' modify constraint "' || ]]..U('PK_NAME')..[[ || '"' || (]]..pk_state..[[) from vv_pk
	UNION ALL select 62, 'alter table ' || ]]..fk_parent..[[ || ' modify constraint "' || ]]..U('FK_NAME')..[[ || '"' || (]]..fk_state..[[) from vv_fk]]..views_union..check_union..[[
) order by ord
]],{})

if not suc then error('"'..res.error_message..'" caught while executing: "'..res.statement_text..'"') end
return(res)
/

-- ===================================================================================================
-- CONNECTION SETUP
-- ===================================================================================================
-- Prerequisites
--   * The SQL Server / Azure SQL database must be reachable from this Exasol database.
--   * The credentials used in the connection must be valid.
--   * Use the latest Microsoft JDBC driver (mssql-jdbc). The legacy jTDS driver is NOT supported - it is
--     unstable with current SQL Server versions and with Azure-hosted databases.
--
-- JDBC driver (install once in BucketFS - driver, its settings.cfg and all dependent jars)
--   * mssql-jdbc 13.4.0.jre11 or higher:
--       https://mvnrepository.com/artifact/com.microsoft.sqlserver/mssql-jdbc
--   * For Azure 'authentication=ActiveDirectoryPassword' additionally install the Azure Identity library
--     (azure-identity 1.18.4 or higher, including all of its dependencies):
--       https://mvnrepository.com/artifact/com.azure/azure-identity
--   * Driver setup guide:
--       https://docs.exasol.com/db/latest/loading_data/connect_sources/sql_server.htm
--
-- Create the connection that matches your environment (adjust host, database name and credentials), then run
-- the accompanying test query. Three common variants are shown.

-- 1) Microsoft SQL Server, on-premises (standard SQL Server authentication)
CREATE OR REPLACE CONNECTION SQLSERVER_JDBC
    TO 'jdbc:sqlserver://sqlserver_host_or_ip:1433;databaseName=mydemo;encrypt=true;trustServerCertificate=true;loginTimeout=30;'
    USER 'user'
    IDENTIFIED BY 'password';
SELECT * FROM (IMPORT FROM JDBC AT SQLSERVER_JDBC STATEMENT 'SELECT ''Connection works'' ');

-- 2) Azure SQL Database (standard SQL authentication)
CREATE OR REPLACE CONNECTION AZURE_SQLSERVER_JDBC
    TO 'jdbc:sqlserver://testserver.database.windows.net:1433;databaseName=mydemo;encrypt=true;trustServerCertificate=true;loginTimeout=30;'
    USER 'user'
    IDENTIFIED BY 'password';
SELECT * FROM (IMPORT FROM JDBC AT AZURE_SQLSERVER_JDBC STATEMENT 'SELECT ''Connection works'' ');

-- 3) Azure SQL Database with Microsoft Entra ID (formerly Azure AD) password authentication.
--    Requires the azure-identity library in BucketFS (see the JDBC driver notes above).
CREATE OR REPLACE CONNECTION AZURE_SQLSERVER_ADPW_JDBC
    TO 'jdbc:sqlserver://testserver.database.windows.net:1433;databaseName=mydemo;encrypt=true;trustServerCertificate=true;loginTimeout=30;authentication=ActiveDirectoryPassword;'
    USER 'your_active_directory_user'
    IDENTIFIED BY 'your_active_directory_password';
SELECT * FROM (IMPORT FROM JDBC AT AZURE_SQLSERVER_ADPW_JDBC STATEMENT 'SELECT ''Connection works'' ');

-- ===================================================================================================
-- GENERATE THE MIGRATION STATEMENTS (recommended defaults shown)
-- ===================================================================================================
EXECUTE SCRIPT database_migration.SQLSERVER_TO_EXASOL(
    'SQLSERVER_JDBC',   -- CONNECTION_NAME:             name of the JDBC connection created above
    false,              -- DB2SCHEMA:                   false (recommended) => "schema"."table"; true => "database"."schema_table" (use when migrating several databases at once)
    'mydemo',           -- DB_FILTER:                   SQL Server database(s): 'mydemo', 'ma%', 'db1, db2', '%' (all)
    '%',                -- SCHEMA_FILTER:               schema(s): 'dbo', 'my%', 'schema1, schema2', '%' (all)
    '',                 -- TARGET_SCHEMA:               Exasol target schema; '' (recommended) => use the source schema (or database) name
    '%',                -- TABLE_FILTER:                table(s)/view(s): 'my_table', 'my%', 't1, t2', '%' (all)
    true,               -- IDENTIFIER_CASE_INSENSITIVE: true (recommended for SQL Server) => fold ALL identifiers to UPPER so Exasol queries never need quotes (SQL Server identifiers are case-insensitive by default, so nothing is lost); false => keep verbatim/quoted (preserves lower/MixedCase, but every query must quote them)
    'FORCE_DISABLE',    -- CONSTRAINT_STATE:            'FORCE_DISABLE' (recommended; PK/FK kept as metadata only - faster imports, order-independent, still used by BI tools), 'SET_AS_SOURCE' (each key ends in its SQL Server state) or 'FORCE_ENABLE' (all keys enabled = Exasol re-validates the data)
    true,               -- GENERATE_COMMENTS:           true (recommended) => migrate MS_Description as COMMENT ON; false => skip
    true,               -- GENERATE_VIEWS:              true => emit source views as a commented manual-review section; false => skip
    true,               -- GENERATE_PARTITION_BY:       true => add a best-effort PARTITION BY (from the SQL Server partitioning column) inside the CREATE TABLE; false => skip
    'HASHTYPE',         -- BINARY_HANDLING:             'HASHTYPE' (recommended; fixed binary -> HASHTYPE, variable -> hex), 'HEX' (always hex VARCHAR) or 'SKIP' (load NULL)
    'CAP',              -- DECIMAL_OVERFLOW:            'CAP' (recommended; DECIMAL(36,s), import fails for values needing > 36 digits) or 'DOUBLE' (loads with ~15 significant digits)
    false,              -- TRUNCATE_LONG_STRINGS:       false (recommended) => import fails on a value > 2,000,000 chars; true => cut such values to 2,000,000 chars and import
    false               -- CHECK_MIGRATION:             false (recommended default) => skip; true => also build "<table>_MIG_CHK" metric tables + a "<schema>_MIG_CHK" summary (source vs target) for post-load validation
);
