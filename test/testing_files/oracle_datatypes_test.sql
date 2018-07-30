-- Creating new schema to do the testing
CREATE TEMPORARY TABLESPACE tbs_temp_01
TEMPFILE 'tbs_temp_01.dbf'
SIZE 5M
AUTOEXTEND ON;

CREATE TABLESPACE tbs_perm_01
DATAFILE 'tbs_perm_01.dat' 
SIZE 20M
ONLINE;

CREATE USER exasol
IDENTIFIED BY oracle
DEFAULT TABLESPACE tbs_perm_01
TEMPORARY TABLESPACE tbs_temp_01
QUOTA 20M on tbs_perm_01;

GRANT create session TO exasol;
GRANT create table TO exasol;
GRANT create view TO exasol;
GRANT create any trigger TO exasol;
GRANT create any procedure TO exasol;
GRANT create sequence TO exasol;
GRANT create synonym TO exasol;

ALTER SESSION SET CURRENT_SCHEMA = exasol;

CREATE TABLE string_types (
  my_char char(50),
  my_nchar nchar(99),
  my_varchar varchar(250),
  my_varchar2 varchar2(25),
  my_nvarchar2 nvarchar2(50),
  my_raw raw(10),
  my_long long,
  --my_longraw long raw (1550),
  my_blob blob
  -- my_clob clob,
  -- my_nclob nclob
);

INSERT INTO string_types VALUES (
  'aaaaaaaaaaaaaaaaaaaa',
  'eza65é&²dffffsqdd!qsdç"e")d!sq:d!:;qsdbbb',
  'cccccccccccccccccrl',
  '12456789123456789123456',
  '33333ezrrezrmlom"émlmmmmsdmfdfé=)"é="zezrzerel',
  '123456789',
  'llllllllllooooooonnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnngggggggggggggggggggggggggggggggggggggggggggggggg',
  utl_raw.cast_to_raw('blllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooobbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb')
  -- 'cllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooobbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb',
  -- 'nclllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooobbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb'
);


CREATE TABLE string_types2 (
  my_long long,
  my_blob blob
  --my_nclob nclob
);

INSERT INTO string_types2 VALUES (
  'llllllllllooooooonnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnngggggggggggggggggggggggggggggggggggggggggggggggg',
  utl_raw.cast_to_raw('blllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooobbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb')
  --'nclllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooobbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb'
);

--SELECT * FROM string_types2;

CREATE TABLE decimal_types (
  my_decimal    decimal(38,38),
  my_decimal2   decimal(10,11),
  my_decimal3   decimal(3,36),
  my_decimal4   decimal(38,10),
  my_decimal5   decimal(15,10)
);
INSERT INTO decimal_types VALUES (0.3232012,0.021121211,0,1234561450.3123,0);
--SELECT * FROM decimal_types;

CREATE TABLE numeric_types (
  my_number number,
  my_number2 number(38),
  my_number3 number(38,38),
  my_number4 number(10,5),
  my_float float,
  my_float2 float(126),
  my_binfloat binary_float,
  my_bindouble binary_double
);

INSERT INTO numeric_types VALUES (
  0,
  12345678012345678,
  0.3333333333,
  12345.01982339, 
  12345.12345, 
  12348.01290156789,
  1234.1241723, 
  1234987.120871234
);
INSERT INTO numeric_types VALUES (
  12312345678,
  0,
  0.12345647894464564655,
  1345.01982339, 
  1234665.12345, 
  12348.01290156789,
  1234.1241723, 
  1234987.120871234
);

--SELECT * FROM numeric_types;


CREATE TABLE date_types (
  my_date date,
  my_timestamp timestamp,
  my_timestamp2 timestamp(3),
  my_timestamp3 timestamp(9),
  my_timestampWTZ timestamp with time zone,
  my_timestampWLTZ timestamp with local time zone,
  my_intervalYM interval year to month,
  my_intervalDS interval day to second
);

INSERT INTO date_types VALUES (
  TO_DATE('2016-08-19', 'YYYY-MM-DD'), 
  TO_TIMESTAMP('2013-03-11 17:30:15.123', 'YYYY-MM-DD HH24:MI:SS.FF'), 
  TO_TIMESTAMP('2013-03-11 17:30:15.123456', 'YYYY-MM-DD HH24:MI:SS.FF'), 
  TO_TIMESTAMP('2013-03-11 17:30:15.123456789', 'YYYY-MM-DD HH24:MI:SS.FF'),
  TO_TIMESTAMP_TZ('2016-08-19 11:28:05 -08:00', 'YYYY-MM-DD HH24:MI:SS TZH:TZM'),
  TO_TIMESTAMP_TZ('2018-04-30 10:00:05 -08:00', 'YYYY-MM-DD HH24:MI:SS TZH:TZM'),
  '54-2',
  '1 11:12:10.123'
);

--SELECT * FROM date_types;


