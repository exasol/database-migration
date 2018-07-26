import sys
import os
import exasol as exa

C = exa.connect(Driver = 'EXAODBC', EXAHOST = os.environ['ODBC_HOST'], EXAUID = os.environ['EXAUSER'], EXAPWD = os.environ['EXAPW'])
#C = exa.connect(dsn='test_dsn')
scriptname = sys.argv[1]
query = ""

if scriptname == "SQLSERVER_TO_EXASOL":
	conn = sys.argv[2]
	dbFilter = sys.argv[3]
	schemaFilter = sys.argv[4]
	tableFilter = sys.argv[5]
	caseSensitive = sys.argv[6]
	query = "execute script database_migration." + scriptname + "('" + conn + "', TRUE, '" + dbFilter + "', '" + schemaFilter + "', '" + tableFilter + "', " + caseSensitive + ")"

else:
	conn = sys.argv[2]
	schemaFilter = sys.argv[3]
	tableFilter = sys.argv[4]
	query = "execute script database_migration." + scriptname + "('" + conn + "', TRUE, '" + schemaFilter + "', '" + tableFilter +"')"
	

f = open("output.sql", "w")
R = C.odbc.execute(query)
output = R.fetchall()
nrows = R.rowcount
for i in range(0, nrows):
	f.write(output[i][0] + "\n")
f.close()
