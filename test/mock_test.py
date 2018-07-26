import exasol as E
import os

C = E.connect(Driver = 'EXAODBC', EXAHOST = os.environ['ODBC_HOST'], EXAUID = os.environ['EXAUSER'], EXAPWD = os.environ['EXAPW'])
#C = E.connect(DSN='EXAODBC_TEST')
R = C.readData("SELECT 'connection works' FROM dual")
print(R)