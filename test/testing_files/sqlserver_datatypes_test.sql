CREATE DATABASE testing;
GO
USE testing;
GO

DROP TABLE IF EXISTS exact_numerics;
CREATE TABLE exact_numerics (
        my_bigint       bigint,
        my_int          int,
        my_smallint     smallint,
        my_tinyint      tinyint,
        my_money        money,
        my_smallmoney   smallmoney,
        my_bit          bit,
        my_decimal      decimal(38,2),
        my_numeric      numeric(10,5)
);

INSERT INTO exact_numerics VALUES (
        -9223372036854775808,
        -2147483648,
        -32768,
        255,
        -922337203685477.5808,
        214748.3647,
        1,
        123456666.126565,
        12345.12
);
GO

--SELECT * FROM exact_numerics;


DROP TABLE IF EXISTS approx_numerics;
CREATE TABLE approx_numerics (
        my_float float(53),
        my_real  real
);

INSERT INTO approx_numerics VALUES (1.698, 1.698);
INSERT INTO approx_numerics VALUES (- 1.79E+308, - 3.40E+38);
INSERT INTO approx_numerics VALUES (1.698,  3.40E+38);
GO

--SELECT * FROM approx_numerics;


DROP TABLE IF EXISTS date_types;
CREATE TABLE date_types (
        my_date         date,
        my_datetime     datetime,
        my_datetime2    datetime2,
        my_datetimeOff  datetimeoffset,
        my_smalldt      smalldatetime,
        my_time         time
);

INSERT INTO date_types VALUES (
        '2018-07-09',
        '1900-01-01 00:00:00',
        '1999-11-11 23:59:59.9999999',
        '1020-01-01 21:59:59.9999999',
        '2007-05-09 23:39:59',
        '23:59:59.9999999'
);

--SELECT my_datetimeOff from date_types;
--SELECT CONVERT(datetime2, my_datetimeOff, 1) AS converted from date_types;

DROP TABLE IF EXISTS datetimeoffset;
CREATE TABLE datetimeoffset (dto datetimeoffset);
INSERT INTO datetimeoffset VALUES ('12-10-25 12:32:10 +01:00'), ('12-10-25 12:32:10 +07:00'), ('12-10-25 12:32:10 +14:00'),
                                  ('12-10-25 12:32:10 -01:00'), ('12-10-25 12:32:10 -07:00'), ('12-10-25 12:32:10 -14:00');
--SELECT * FROM datetimeoffset;

DROP TABLE IF EXISTS string_types;
CREATE TABLE string_types (
        my_char       char(15),
        my_varchar    varchar(15),
        my_varchar2   varchar(max),
        my_text       text,
        my_nchar      nchar(10),
        my_nvarchar   nvarchar(max),
        my_ntext      ntext
);

INSERT INTO string_types VALUES (
        '123456789123456',
         'test',
         'In SQL Server, each column, local variable, expression, and parameter has a related data type. A data type is an attribute that specifies the type of data that the object can hold: integer data, character data, monetary data, date and time data, binary strings, and so on.',
         'In SQL Server, each column, local variable, expression, and parameter has a related data type. A data type is an attribute that specifies the type of data that the object can hold: integer data, character data, monetary data, date and time data, binary strings, and so on.',
         '1234567891',
         'In SQL Server, each column, local variable, expression, and parameter has a related data type. A data type is an attribute that specifies the type of data that the object can hold: integer data, character data, monetary data, date and time data, binary strings, and so on.',
         'In SQL Server, each column, local variable, expression, and parameter has a related data type. A data type is an attribute that specifies the type of data that the object can hold: integer data, character data, monetary data, date and time data, binary strings, and so on.' 
);

--SELECT * FROM string_types;


DROP TABLE IF EXISTS bin_string_types;
CREATE TABLE bin_string_types (
        my_binary       binary(10),
        my_varbinary    varbinary(max)
);

INSERT INTO bin_string_types VALUES (
        CAST('0x0001e240' AS BINARY),
        CAST('jdjkskfdsklfjkdsfjdqkldlslfkjdsflk' AS VARBINARY(MAX))
);


DROP TABLE IF EXISTS bin2_string_types;
CREATE TABLE bin2_string_types (
        my_binary       binary(10),
        string          varchar(10),
        my_varbinary    varbinary(max)
);

INSERT INTO bin2_string_types VALUES (
        CAST('0x0001e240' AS BINARY),
        'test',
        CAST('jdjkskfdsklfjkdsfjdqkldlslfkjdsflk' AS VARBINARY(MAX))
);
        
--SELECT * FROM bin_string_types;


DROP TABLE IF EXISTS spatial_types;
CREATE TABLE spatial_types (
        my_geom         geometry,
        my_geog         geography
);

INSERT INTO spatial_types VALUES (
        geometry::STGeomFromText('LINESTRING (100 100, 20 180, 180 180)', 0),
        geography::STGeomFromText('LINESTRING(-122.360 47.656, -122.343 47.656 )', 4326)
);

INSERT INTO spatial_types VALUES (
        geometry::STGeomFromText('POLYGON ((0 0, 150 0, 150 150, 0 150, 0 0))', 0),
        geography::STGeomFromText('POLYGON((-122.358 47.653 , -122.348 47.649, -122.348 47.658, -122.358 47.658, -122.358 47.653))', 4326)
);

--SELECT * FROM spatial_types;


DROP TABLE IF EXISTS other_types;
CREATE TABLE other_types (
        my_hierarchyid  hierarchyid,
        my_xml          xml
);

INSERT INTO other_types VALUES 
('/1/', 
'<note>
  <to>Tove</to>
  <from>Jani</from>
  <heading>Reminder</heading>
  <body>Dont forget me this weekend!</body>
</note>'
),  
('/2/', 
'<food>
    <name>Strawberry Belgian Waffles</name>
    <price>$7.95</price>
    <description>
    Light Belgian waffles covered with strawberries and whipped cream
    </description>
    <calories>900</calories>
</food>'
),  
('/1/1/', 
'<food>
    <name>Berry-Berry Belgian Waffles</name>
    <price>$8.95</price>
    <description>
    Belgian waffles covered with assorted fresh berries and whipped cream
    </description>
    <calories>900</calories>
</food>'
),   
('/2/1/2/1/', 
'<food>
    <name>Homestyle Breakfast</name>
    <price>$6.95</price>
    <description>
    Two eggs, bacon or sausage, toast, and our ever-popular hash browns
    </description>
    <calories>950</calories>
</food>'
);    

--SELECT * FROM other_types;

DROP TABLE IF EXISTS long_numeric;
CREATE TABLE long_numeric (my_long numeric(38,38));
INSERT INTO long_numeric VALUES 
(-0.123456789123456789123456789125678912),(0.100000000000000000000000000000000000),(0.123456789123458912345678912345678912);
--SELECT * FROM long_numeric;

DROP TABLE IF EXISTS long_decimal;
CREATE TABLE long_decimal (my_long decimal(38));
INSERT INTO long_decimal VALUES 
(-123456789123456789123456789123456789),(100000000000000000000000000000000000),(345678912345678912345678912345678912);
--SELECT * FROM long_decimal;
        
