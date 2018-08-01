DROP SCHEMA IF EXISTS testing_datatypes_schema CASCADE;
CREATE SCHEMA testing_datatypes_schema;
SET search_path = testing_datatypes_schema;

DROP TABLE IF EXISTS numeric_types CASCADE;
CREATE TABLE numeric_types (
        my_smallint     smallint,
        my_int          integer,
        my_bigint       bigint,
        my_decimal      decimal(65,30),
        my_numeric      numeric(65,30),
        my_real         real,
        my_double       double precision,
        my_smallserial  smallserial,
        my_serial       serial,
        my_bigserial    bigserial
);

INSERT INTO numeric_types VALUES (
        32766,
        214748366,
        9223372036854775806,
        123,
        1231,
        7.365465,
        12.335665657
);
        
        
--SELECT * FROM testing_datatypes_schema.numeric_types;


DROP TABLE IF EXISTS decimal_types CASCADE;
CREATE TABLE decimal_types (
  my_decimal    decimal(38,38),
  my_decimal2   decimal(151,111),
  my_decimal3   decimal(43,43),
  my_decimal4   decimal(38,10),
  my_decimal5   decimal(15,10)
);
INSERT INTO decimal_types VALUES (0.3232012,0.021121211,0,1234561450.3123,0);
--SELECT * FROM decimal_types;


DROP TABLE IF EXISTS money CASCADE;
CREATE TABLE money (balance money, bool boolean);
INSERT INTO money VALUES (-92233720368547758.08, TRUE);
INSERT INTO money VALUES (92233720368547758.07, '1');
INSERT INTO money VALUES (100000, 'no');
INSERT INTO money VALUES (10000000, 'y');
INSERT INTO money VALUES (100000000, 'off');
INSERT INTO money VALUES (1000000000, '0');
--SELECT * FROM money;

DROP TABLE IF EXISTS character_types CASCADE;
CREATE TABLE character_types (
        my_varchar varchar,
        my_char char(1),
        my_character character(3),
        my_text text
);

INSERT INTO character_types VALUES (
        'Hello',
        'T',
        'SQL',
        'SQL defines two primary character types: character varying(n) and character(n), where n is a positive integer. Both of these types can store strings up to n characters (not bytes) in length. An attempt to store a longer string into a column of these types will result in an error, unless the excess characters are all spaces, in which case the string will be truncated to the maximum length. (This somewhat bizarre exception is required by the SQL standard.) If the string to be stored is shorter than the declared length, values of type character will be space-padded; values of type character varying will simply store the shorter string.'
);

--SELECT * FROM character_types;   

DROP TABLE IF EXISTS bytea CASCADE;
CREATE TABLE bytea (my_bytea bytea);
INSERT INTO bytea VALUES (
        'A binary string is a sequence of octets (or bytes)'
);
--SELECT * FROM bytea;

DROP TABLE IF EXISTS date_types CASCADE;
CREATE TABLE date_types (
        my_timestamp    timestamp,
        my_timestamptz  timestamptz,
        my_date         date,
        my_time         time,
        my_timetz       timetz,
        my_interval     interval
);

INSERT INTO date_types VALUES (
        '2004-10-19 10:23:54+02',
        '2004-10-19 10:23:54',
        'today',
        '1999-01-08 04:05:06',
        '1999-01-08 04:05:06 -8:00',
        '1 year 2 months 3 days 4 hours 5 minutes 6 seconds'
);
INSERT INTO date_types VALUES (
        '2004-10-19 10:23:54+02',
        '2004-10-19 10:23:54',
        'yesterday',
        '1999-01-08 04:05:06',
        '1999-01-08 04:05:06 -8:00',
        '3 4:05:06'
);

--SELECT * FROM date_types;


DROP TABLE IF EXISTS geometric_types CASCADE;
CREATE TABLE geometric_types (
        my_point      point,
        my_line       line,
        my_lseg       lseg,
        my_box        box,
        my_path       path,
        my_polygon    polygon,
        my_circle     circle
);
INSERT INTO geometric_types VALUES (
        '(1,2)',
        '{3,9,4}',
        '[ ( 1 , 2 ) , ( 5 , 6 ) ]',
        '( 0 , 1 ) , ( 11 , 12 )',
        '[( 1 , 2 ) , ( 5 , 6 ), ( 0 , 1 ) , ( 11 , 12 )]',
        '(( 1 , 2 ) , ( 5 , 6 ), ( 0 , 1 ) , ( 11 , 12 ))',
        '( ( 0 , 0 ) , 2 )'
);
--SELECT * FROM geometric_types; 

DROP TABLE IF EXISTS networkaddr_types CASCADE;
CREATE TABLE networkaddr_types (
        my_cidr      cidr,
        my_inet      inet,
        my_macaddr   macaddr
);      
INSERT INTO networkaddr_types VALUES ('192.168.100.128/25', '192.168.100.128/25', '08:00:2b:01:02:03');
INSERT INTO networkaddr_types VALUES ('2001:4f8:3:ba::/64', '::ffff:1.2.3.0/128', '08002b:010203');
INSERT INTO networkaddr_types VALUES ('128.1', '10.1.2.0/24', '0800.2b01.0203');
--SELECT * FROM networkaddr_types;
        
DROP TABLE IF EXISTS bitstring_types CASCADE;
CREATE TABLE bitstring_types (a BIT(3), b BIT VARYING(5));
INSERT INTO bitstring_types VALUES (B'101', B'00');
INSERT INTO bitstring_types VALUES (B'100', B'101');
INSERT INTO bitstring_types VALUES (B'100'::bit(3), B'101');
--SELECT * FROM bitstring_types;

DROP TABLE IF EXISTS xmljson CASCADE; 
CREATE TABLE xmljson (my_xml xml, my_json json);
INSERT INTO xmljson VALUES (
        '<?xml version="1.0"?><book><title>Manual</title><chapter>...</chapter></book>',
        '{"menu": {
          "id": "file",
          "value": "File",
          "popup": {
            "menuitem": [
              {"value": "New", "onclick": "CreateNewDoc()"},
              {"value": "Open", "onclick": "OpenDoc()"},
              {"value": "Close", "onclick": "CloseDoc()"}
            ]
          }
        }}'
);
--SELECT * FROM xmljson;


CREATE TYPE weekday AS ENUM ('Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun');
CREATE TYPE inventory_item AS (
   name text,
   supplier_id integer,
   price numeric
);
DROP TABLE IF EXISTS other_types CASCADE; 
CREATE TABLE other_types (item inventory_item, dayOfWeek weekday);
INSERT INTO other_types VALUES (
        ROW('fuzzy dice', 42, 1.99),
        'Mon'
);
--SELECT * FROM other_types;