CREATE DATABASE IF NOT EXISTS testing_datatypes_schema;

USE testing_datatypes_schema;

DROP TABLE IF EXISTS numeric_types CASCADE;
CREATE TABLE numeric_types (
        my_tinyint      tinyint NOT NULL,
        my_smallint     smallint,
        my_mediumint    mediumint,
        my_int          int(4),
        my_bigint       bigint,
        my_decimal      decimal(65,30),
        my_float        float(30,20),
        my_double       double
);

INSERT INTO numeric_types (my_tinyint, my_smallint, my_mediumint, my_int, my_bigint, my_decimal, my_float, my_double) VALUES (
        '-128',
        '32000',
        '-8388608',
        '94967295',  
        '42949672954295',
        '12312.1231231231',
        '1299.9999993',
        '12.3'
);


DROP TABLE IF EXISTS decimal_types CASCADE;
CREATE TABLE decimal_types (
  my_decimal    decimal(38,30),
  my_decimal2   decimal(16,11),
  my_decimal3   decimal(59,30),
  my_decimal4   decimal(38,10),
  my_decimal5   decimal(15,10)
);
INSERT INTO decimal_types VALUES (0.3232012,0.021121211,0,1234561450.3123,0);


DROP TABLE IF EXISTS test_bit CASCADE;
CREATE TABLE test_bit (my_bit bit);
INSERT INTO test_bit (my_bit) VALUES (b'00001');
INSERT INTO test_bit(my_bit) VALUES (b'00000');
INSERT INTO test_bit(my_bit) VALUES (b'1');


DROP TABLE IF EXISTS string_types CASCADE;
CREATE TABLE string_types (
        my_char         char(255),
        my_varchar      varchar(255),
        my_binary       binary(255),
        my_varbinary    varbinary(255),
        my_tinyblob     tinyblob,
        my_blob         blob,
        my_mediumblob   mediumblob,
        my_longblob     longblob,
        my_tinytext     tinytext,
        my_text         text,
        my_mediumtext   mediumtext,
        my_longtext     longtext,
        my_enum         ENUM('x-small', 'small', 'medium', 'large', 'x-large'),
        my_set          SET('a', 'b', 'c', 'd')
);

INSERT INTO string_types (my_char, my_varchar, my_binary, my_varbinary,
                          my_tinyblob, my_blob, my_mediumblob, my_longblob, 
                          my_tinytext, my_text, my_mediumtext, my_longtext, my_enum, my_set) 
VALUES (
        'abc','abc', 'abc', '123',
        'tinyblob', 'A Binary Large OBject (BLOB) is a collection of binary data stored as a single entity in a database management system. Blobs are typically images, audio or other multimedia objects, though sometimes binary executable code is stored as a blob. Database support for blobs is not universal.','mediumblob','longblob',
        'tinytext','text','mediumtext','longtext', 'large', 'a'
        
);

        
DROP TABLE IF EXISTS date_types CASCADE;
CREATE TABLE date_types (
        my_date         date,
        my_datetime     datetime,
        my_timestamp    timestamp,
        my_time         time,
        my_year         year
);

INSERT INTO date_types (my_date, my_datetime, my_timestamp, my_time, my_year) VALUES (
        '2000-01-31',
        '11-11-11 11:11:11',
        '12-12-12 11:12:12',
        '11:59:59',
        '2003'
);

DROP TABLE IF EXISTS test_time CASCADE;
CREATE TABLE test_time (my_time time);
INSERT INTO test_time VALUES ('3:03:03');
INSERT INTO test_time VALUES ('13:03:03');
INSERT INTO test_time VALUES ('23:03:03');
INSERT INTO test_time VALUES ('11:03:03');

DROP TABLE IF EXISTS test_year CASCADE;
CREATE TABLE test_year (my_year year(4));
INSERT INTO test_year VALUES ('3');
INSERT INTO test_year VALUES ('13');
INSERT INTO test_year VALUES ('23');
INSERT INTO test_year VALUES ('33');

        
DROP TABLE IF EXISTS spatial_types CASCADE;
CREATE TABLE spatial_types (
        my_geometry     geometry,
        my_point        point,
        my_linestring   linestring,
        my_polygon      polygon,
        my_gc           geometrycollection
);


SET @g = 'POINT(1 1)';
SET @point = 'POINT(1 1)';
SET @linestring = 'LINESTRING(0 0,1 1,2 2)';
SET @polygon = 'POLYGON((0 0,10 0,10 10,0 10,0 0),(5 5,7 5,7 7,5 7, 5 5))';
SET @gc = 'GEOMETRYCOLLECTION(POINT(1 1),LINESTRING(0 0,1 1,2 2,3 3,4 4))';

INSERT INTO spatial_types (my_geometry, my_point, my_linestring, my_polygon, my_gc) VALUES (
        GeomFromText(@g),
        PointFromText(@point),
        LineStringFromText(@linestring),
        PolygonFromText(@polygon),
        ST_GeomFromText(@gc)
);



DROP TABLE IF EXISTS test_json CASCADE;
CREATE TABLE test_json (my_json JSON);
INSERT INTO test_json (my_json) VALUES (
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


DROP TABLE IF EXISTS test_json2 CASCADE;
CREATE TABLE test_json2 (id int, my_json JSON);
INSERT INTO test_json2 (id, my_json) VALUES (1,
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


DROP TABLE IF EXISTS test_json3 CASCADE;
CREATE TABLE test_json3 (id int, my_json JSON, string varchar(20));
INSERT INTO test_json3 (id, my_json, string) VALUES (1,
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
        }}',
        'hello!'
);


DROP TABLE IF EXISTS test_json4 CASCADE;
CREATE TABLE test_json4 (my_json JSON, another_json JSON, string varchar(20));
INSERT INTO test_json4 (my_json, another_json, string) VALUES (
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
        }}',
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
        }}',
        'hello!'
);

