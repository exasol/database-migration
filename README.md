# database-migration

###### Please note that this is an open source project which is *not officially supported* by EXASOL. We will try to help you as much as possible, but can't guarantee anything since this is not an official EXASOL product.

## Table of Contents
1. [Overview](#overview)
2. [Post load optimization](#post-load-optimization)
3. [Delta import](#delta-import)


## Overview

This project contains SQL scripts for automatically importing data from various data management systems into EXASOL.

You'll find a list of SQL scripts which you can execute on EXASOL to load data from certain databases or
database management systems. The scripts try to extract the meta data from the sources and create the
appropriate IMPORT statements automatically so that you don't have to care about table names and column
names and types.

If you want to optimize existing scripts or create new scripts for additional systems, we would be very
glad if you share your work with the EXASOL user community.


## Post load optimization
This folder contains scripts that can be used after having imported data from another database via the scripts above.
What they do:
- Optimize the column's datatypes to minimize storage space on disk
- Import primary keys from other databases


## Delta import
This folder contains a script that can be used if you want to import data on a regular basis.
What it does:
- Import only data that hasn't been imported yet by performing a delta import based on a given column (further explaination [inside the folder](delta_import))
