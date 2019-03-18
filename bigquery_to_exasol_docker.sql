CREATE SCHEMA IF NOT EXISTS database_migration;

/*
    This script will generate create schema, create table, create import and create connection statements
    to load all needed data from Google BigQuery. Automatic datatype conversion is
    applied whenever needed. Feel free to adjust it.

    PREREQUISITES:
    BigQuery service account
    Put the following files into a bucket called bqmigration:
    - JSON file to authenticate with service account
    - Google BigQuery JDBC driver files (tested with Simba version 1.1.6)

    CONVERSION:
    - BQ dataset -> Exasol schema
    - BQ table      -> Exasol table
*/
--/
CREATE OR REPLACE JAVA SCALAR SCRIPT DATABASE_MIGRATION.BIGQUERY_TO_EXASOL(
    SERVICE_ACCOUNT            VARCHAR(1000)
  , KEY_NAME                   VARCHAR(1000)
  , PROJECT_ID                 VARCHAR(1000)
  , CONNECTION_NAME            VARCHAR(100)
  , IDENTIFIER_CASE_SENSITIVE  BOOLEAN
  , SCHEMA_FILTER              VARCHAR(1000)
  , TABLE_FILTER               VARCHAR(1000)
) EMITS (DDL VARCHAR(2000000)) AS

%jar /buckets/bfsdefault/default/google-api-client-1.23.0.jar;
%jar /buckets/bfsdefault/default/google-api-services-bigquery-v2-rev377-1.23.0.jar;
%jar /buckets/bfsdefault/default/GoogleBigQueryJDBC42.jar;
%jar /buckets/bfsdefault/default/google-http-client-1.23.0.jar;
%jar /buckets/bfsdefault/default/google-http-client-jackson2-1.23.0.jar;
%jar /buckets/bfsdefault/default/google-oauth-client-1.23.0.jar;
%jar /buckets/bfsdefault/default/jackson-core-2.1.3.jar;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import com.simba.googlebigquery.jdbc42.DataSource;

class BIGQUERY_TO_EXASOL {
    static Connection con;
    static List<TableMetaData> tableList;
    static Set<String> schemaSet;
    static DatabaseMetaData databaseMetaData;
    static HashMap<Integer,String> mapping;

    static class TableMetaData{
        String schema;
        String table;

        public TableMetaData(String schema, String table){
            this.schema=schema;
            this.table=table;
        }

        public String getSchema(){
            return this.schema;
        }

        public String getTable(){
            return this.table;
        }
    }

    static void initVariables(){
        mapping = new HashMap<Integer,String>();
        mapping.put(java.sql.Types.BOOLEAN, "BOOLEAN");
        mapping.put(java.sql.Types.VARBINARY,"VARCHAR(2000000)");
        mapping.put(java.sql.Types.DATE,"DATE");
        mapping.put(java.sql.Types.VARCHAR,"VARCHAR(100000)");
        mapping.put(java.sql.Types.DOUBLE,"DOUBLE");
        mapping.put(java.sql.Types.BIGINT,"INTEGER");
        mapping.put(java.sql.Types.NUMERIC,"DECIMAL(36,9)");
        mapping.put(java.sql.Types.TIME,"VARCHAR(100)");
        mapping.put(java.sql.Types.TIMESTAMP,"TIMESTAMP");
        tableList = new LinkedList<TableMetaData>();
        schemaSet = new HashSet<String>();
    }

    static String createURL(String project_name,String account,String key_name, boolean internal){
        StringBuffer buffer = new StringBuffer();
        buffer.append("jdbc:");
        if(internal){
            buffer.append("bigquery");
        }
        else{
            buffer.append("exaquery");
        }
        buffer.append("://https://www.googleapis.com/bigquery/v2:443;");
        buffer.append("ProjectId="+project_name+";");
        buffer.append("OAuthType=0;");
        buffer.append("Timeout=10000;");
        buffer.append("OAuthServiceAcctEmail="+account+";");
        buffer.append("OAuthPvtKeyPath=");
        if(internal){
            buffer.append("/buckets/bfsdefault/default/");
        }
        else{
            buffer.append("/d02_data/bfsdefault/default/");
        }
        buffer.append(key_name+";");
        return buffer.toString();
    }

    static void connectBQ(String URL)throws SQLException{
        DataSource ds = new com.simba.googlebigquery.jdbc42.DataSource();
        ds.setURL(URL);
        con = ds.getConnection();
    }

    static void getTables(String schema_filter, String table_filter) throws SQLException{
        databaseMetaData = con.getMetaData();
        ResultSet result = databaseMetaData.getTables(null, null, null, null);
        while(result.next()) {
            String curSchema = result.getString(2);
            String curTable = result.getString(3);
            if(curSchema.matches(schema_filter) && curTable.matches(table_filter)){
                schemaSet.add(curSchema);
                tableList.add(new TableMetaData(curSchema, curTable));
            }
        }
    }

    static void generateDDL(String ConnectionName, String Project, ExaIterator ctx, boolean case_sensitive) throws Exception {
        Iterator<String> schemaIt = schemaSet.iterator();
        ctx.emit("-- SCHEMATA");
        while(schemaIt.hasNext()){
            ctx.emit("CREATE SCHEMA "+schemaIt.next()+";");
        }
        ctx.emit("-- TABLES");
        Iterator<TableMetaData> tableIt = tableList.listIterator();
        while (tableIt.hasNext()){
            TableMetaData entry = tableIt.next();
            String tableName = entry.getTable();
            if(case_sensitive){
                tableName = "\""+tableName+"\"";
            }
            ctx.emit("CREATE TABLE "+entry.getSchema()+"."+tableName+" (");
            ResultSet result = databaseMetaData.getColumns(null, entry.getSchema(),  entry.getTable(), null);
            boolean more = result.next();
            while(more){
                String attr = new String();
                attr=result.getString(4)+" "+ mapping.get(result.getInt(5));
                more = result.next();
                if(more){
                    attr = attr +(",");
                }
                ctx.emit(attr);
            }
            ctx.emit(");");
            ctx.emit("IMPORT INTO "+entry.getSchema()+"."+tableName+" FROM JDBC AT "+ConnectionName+" STATEMENT 'SELECT * FROM `"+Project+"."+entry.getSchema()+"."+entry.getTable()+"`';");
        }
    }

    static String createConnectionStatement(String CONNECTION_NAME, String URL){
        return "CREATE CONNECTION "+CONNECTION_NAME+" TO '" + URL +"';";
    }

    static void run(ExaMetadata exa, ExaIterator ctx) throws Exception {
        initVariables();
        connectBQ(createURL(ctx.getString("PROJECT_ID"), ctx.getString("SERVICE_ACCOUNT"), ctx.getString("KEY_NAME"), true));
        getTables(ctx.getString("SCHEMA_FILTER"), ctx.getString("TABLE_FILTER"));
        ctx.emit(createConnectionStatement(ctx.getString("CONNECTION_NAME"), createURL(ctx.getString("PROJECT_ID"), ctx.getString("SERVICE_ACCOUNT"), ctx.getString("KEY_NAME"), false)));
        generateDDL(ctx.getString("CONNECTION_NAME"), ctx.getString("PROJECT_ID"), ctx, ctx.getBoolean("IDENTIFIER_CASE_SENSITIVE"));
    }
}
/

-- Finally start the import process (JDBC connection object is created as part of the script)
SELECT DATABASE_MIGRATION.BIGQUERY_TO_EXASOL (
'yourproject@yourserviceaccount.iam.gserviceaccount.com', -- BigQuery service account
'yourcredentials.json', -- name of the credentials file (in BucketFS see header)
'yourproject', -- BigQuery project
'BQ_MIGRATE', -- name of JBDC connection to be created for consecutive imports
true, -- case sensitivity handling for identifiers -> false: handle them case sensitiv / true: handle them case insensitiv --> recommended: true
'.*', -- schema filter -> '.*' to load all BQ datasets (JAVA regexp syntax)
'.*' -- table filter -> '.*' to load all BQ tables (JAVA regexp syntax)
);
