# Post load optimizations

This folder contains scripts that can be used after having imported data from another database.
What they do:
- Optimize the column's datatypes to minimize storage space on disk and to speed up joins, see section
- Import primary keys from other databases


## Table of Contents
1. [Optimize datatypes](#optimize_datatypes)
2. [Migrate primary keys](#migrate_primary_keys)


## Optimize datatypes
See script [convert_datatypes.sql](convert_datatypes.sql)
This script helps you to get optimal datatypes for your tables.
It performs multiple optimizations:

#### DOUBLE --> DECIMAL
Convert columns of type `DOUBLE` to `DECIMAL` if the column only contains integers

###### Example:

| Current datatype | Example value | New datatype  |
|------------------|---------------|---------------|
| DOUBLE           | 1234567890.0  | DECIMAL(18,0) |

#### DECIMAL --> smaller DECIMAL
Convert columns of type `DECIMAL` to a smaller `DECIMAL` if the column only contains values, that would also fit into a smaller type. The script won't pick the smallest DECIMAL possible but will either choose DECIMAL(18,0) or DECIMAL(9,0) as this corresponds to int32 / int64.
So far, it will only consider columns without scale.

###### Example:

| Current datatype | Length of maximum value (abs) | New datatype  |
|------------------|-------------------------------|---------------|
| DECIMAL(20,0)    | 17                            | DECIMAL(18,0) |
| DECIMAL(18,0)    | 2                             | DECIMAL(9,0)  |


#### TIMESTAMP --> DATE
If a column is of type `TIMESTAMP` but only contains values where the time of the day is set to 0, it will be converted to `DATE`.

###### Example:

| Current datatype | Example value           | New datatype |
|------------------|-------------------------|--------------|
| TIMESTAMP        | 2018-02-09 00:00:00.000 | DATE         |



#### VARCHAR --> smaller VARCHAR
Convert columns of type `VARCHAR` to a smaller `VARCHAR` if the column only contains values, that would also fit into a smaller type. The script won't take the smallest possible length but will add 20% (and then take the next bigger round decimal) buffer to make sure, inserts with slightly larger values will still work.

###### Example:
| Current datatype | Length of maximum value (abs) | New datatype  |
|------------------|-------------------------------|---------------|
| VARCHAR(2000000) | 1543                          | VARCHAR(2000) |
| VARCHAR(1000)    | 7                             | VARCHAR(10)   |


#### Parameters to be aware of:
``` SQL
execute script DATABASE_MIGRATION.CONVERT_DATATYPES(
'DATATYPES',		-- schema_name: 	 SCHEMA name or SCHEMA_FILTER (can be %)
'%', 		   	-- table_name: 	  TABLE name or TABLE_FILTER  (can be %)
false   		    -- apply_conversion: If false, only output of what would be changed is generated, if true conversions are applied
);

```

* `SCHEMA_NAME`: The schema you want to modify, wildcards are allowed.

* `TABLE_NAME`: The table you want to modify, wildcards are allowed. To execute the script on a whole schema, put `'%'`.

* `APPLY_CONVERSION`: This parameter specifies if you directly want to the script to modify the tables. If set to `true`, it will apply the conversions. For `false`, you will get the SQL statements that would be executed as an output and can still modify them manually.

#### Example for convert_datatypes:

``` SQL
create schema if not exists DATATYPES;
CREATE OR REPLACE TABLE DATATYPES.NUMBER_TEST (
    "DOUBLE" DOUBLE,
    DOUBLE_TO_CONVERT DOUBLE,
	DOUBLE_TO_CONVERT2 DOUBLE,
	DOUBLE_NULL DOUBLE,
    TIMESTAMP_REAL TIMESTAMP,
	TIMESTAMP_TO_CONVERT TIMESTAMP,
	TIMESTAMP_TO_KEEP_WITH_NULL TIMESTAMP,
	"TIMESTAMP" TIMESTAMP,
	"WEIRDCOL'NAME1" DOUBLE,
	"WEIRDCOL'NAME2" DECIMAL(18),
	"WEIRDCOL'NAME3" TIMESTAMP
);

INSERT INTO DATATYPES.NUMBER_TEST VALUES
(1.2,1,1.000000000001,null,'1000-01-01 00:00:00.000', '1000-01-01 00:00:00.000', '1999-01-01 00:00:00.001', '1000-01-01 00:00:01.000',1,2, '1000-01-01 00:00:00.000')
,(2.1,2,2.000000000001,null,'2012-01-01 01:23:31.000', '1999-01-01 00:00:00.000', null, '1999-01-01 23:59:59.999',1,2, '1000-01-01 00:00:00.000' )
;

CREATE OR REPLACE TABLE DATATYPES.VARCHAR_TEST (
	"SMALL_VARCHAR" VARCHAR(2000),
	"BIG_VARCHAR" VARCHAR(2000000),
	"VARCHAR_TO_KEEP" VARCHAR(10),
	"VARCHAR_EMPTY" VARCHAR(1000)
);

INSERT INTO DATATYPES.VARCHAR_TEST VALUES
('a', 'onehundred_chars_bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb', 'ten_bbbbbb', null)
;


-- If executed with 'false' --> Script only displays what changes would be made
execute script DATABASE_MIGRATION.CONVERT_DATATYPES(
'DATATYPES',		-- schema_name: 	 SCHEMA name or SCHEMA_FILTER (can be %)
'%', 		   	-- table_name: 	  TABLE name or TABLE_FILTER  (can be %)
false   		    -- apply_conversion: If false, only output of what would be changed is generated, if true conversions are applied
);
```

## Migrate primary keys
See script [set_primary_keys.sql](set_primary_keys.sql)
