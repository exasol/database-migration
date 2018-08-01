CONNECT TO SAMPLE;
CREATE SCHEMA TESTING;
SET SCHEMA TESTING; 

CREATE TABLE numerics (
        my_bigint       bigint,
        my_int          integer,
        my_smallint     smallint,
        my_real         real,
        my_double       double,
        my_float        float,             
        my_decimal      decimal(31,2),
        my_numeric      numeric(10,5),
        my_decfloat     decfloat
);

INSERT INTO numerics VALUES (-9223372036854775807, 2147483647, -32768, '0.000000012', '0.000000012', '0.000000012', '123456789.12', 0, '0.000000012');
-- SELECT * FROM numerics;

CREATE TABLE character_strings (
        my_char       character(15),
        my_varchar    varchar(200),
        my_clob       clob,
        my_graphic    graphic(10),
        my_vargraphic vargraphic(10),
        my_dbclob     dbclob,
        my_blob       blob  
);
INSERT INTO character_strings VALUES (
        '123456789123456',
        'hello',
        'DB2 compares values of different types and lengths. A comparison occurs when both values are numeric, both values are character strings, or both values are graphic strings. Comparisons can also occur between character and graphic data or between character and datetime data if the character data is a valid character representation of a datetime value. Different types of string or numeric comparisons might have an impact on performance.',
        graphic('TEST', 4),
        vargraphic('TEST', 4),
        null,
        null
);
-- SELECT * FROM character_strings;

CREATE TABLE date_types (
        my_date         date,
        my_time         time,
        my_ts           timestamp
);

INSERT INTO date_types VALUES (
        '2018-07-09',
        '10:00:00',
        '1020-01-01 21:59:59.9999999'
);
-- SELECT * FROM date_types;


CREATE TABLE xml_table (
        my_id           varchar(10),
        my_xml          xml
);

INSERT INTO xml_table VALUES 
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

-- SELECT * FROM xml_table;


CREATE TABLE xml_table2 (my_xml xml);
INSERT INTO xml_table2 VALUES 
(
'<note>
  <to>Tove</to>
  <from>Jani</from>
  <heading>Reminder</heading>
  <body>Dont forget me this weekend!</body>
</note>'
);
-- SELECT * FROM xml_table2;

CREATE DISTINCT TYPE TESTING.US_DOLLAR AS DECIMAL (9,2);
CREATE DISTINCT TYPE TESTING.CANADIAN_DOLLAR AS DECIMAL (9,2);

CREATE TABLE sales(
   sales_id     integer,
   amount_us    TESTING.US_DOLLAR,
   amount_can   TESTING.CANADIAN_DOLLAR
);
INSERT INTO sales VALUES (1, 1000.45, 999.99);
-- SELECT * FROM sales;