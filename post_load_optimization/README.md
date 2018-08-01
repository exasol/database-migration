# Post load optimizations

This folder contains scripts that can be used after having imported data from another database.
What they do:
- Optimize the column's datatypes to minimize storage space on disk and to speed up joins, see section
- Import primary keys from other databases


## Table of Contents
1. [Optimize datatypes](#convert_datatypes)
2. [Migrate primary keys](#set_primary_keys)


## Optimize datatypes
See script [convert_datatypes.sql](convert_datatypes.sql)

Example for convert_datatypes:

``` SQL
create schema if not exists DATATYPES;
CREATE OR REPLACE TABLE DATATYPES.DATATYPE_TEST (
    "DOUBLE" DOUBLE,
    DOUBLE_TO_CONVERT DOUBLE,
	DOUBLE_TO_CONVERT2 DOUBLE,
	DOUBLE_NULL DOUBLE,
    TIMESTAMP_REAL TIMESTAMP,
	TIMESTAMP_TO_CONVERT TIMESTAMP,
	TIMESTAMP_TO_KEEP_WITH_NULL TIMESTAMP,
	"TIMESTAMP" TIMESTAMP,
	"WEIRDCOL'NAME1" DOUBLE,
	"WEIRDCOL'NAME2" DECIMAL(9),
	"WEIRDCOL'NAME3" TIMESTAMP
);

INSERT INTO DATATYPES.DATATYPE_TEST VALUES
(1.2,1,1.000000000001,null,'1000-01-01 00:00:00.000', '1000-01-01 00:00:00.000', '1999-01-01 00:00:00.001', '1000-01-01 00:00:01.000',1,2, '1000-01-01 00:00:00.000')
,(2.1,2,2.000000000001,null,'2012-01-01 01:23:31.000', '1999-01-01 00:00:00.000', null, '1999-01-01 23:59:59.999',1,2, '1000-01-01 00:00:00.000' )
;
```

## Migrate primary keys
See script [set_primary_keys.sql](set_primary_keys.sql)
