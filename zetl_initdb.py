"""
  Dave Skura, 2022
"""
from postgresdatabase import db
import psycopg2 
import sys

zetldb = db()

zetl_tables_required = ['z_etl','z_log','z_activity']


def	createtables(tables_list):
	zetldb.connect()
	for i in range(0,len(tables_list)):
		print('Looking for table: ' + tables_list[i])

		if not zetldb.does_table_exist(tables_list[i]):

			print(tables_list[i] + ' Not Found.')
			DDLFile = '.\\install_ddl\\' + tables_list[i] + '.ddl'
			print('Creating from ' + DDLFile)

			fh = open(DDLFile,'r')
			ddl = fh.read()
			fh.close()

			# add schema prefix
			ddl = ddl.replace('CREATE TABLE ','CREATE TABLE ' + zetldb.ischema + '.')
			ddl = ddl.replace('COMMENT ON TABLE ','COMMENT ON TABLE ' + zetldb.ischema + '.')
			try:
				zetldb.execute(ddl)
				print(tables_list[i] + ' Created. ')
			except Exception as e:
				raise Exception('cannot create tables.  ' + str(e))

		else:
			print(tables_list[i] + ' Found.')
				
	zetldb.close()

if len(sys.argv) == 1 or sys.argv[1] == 'zetl.py':
	raise Exception('This program needs 1 argument.  [check_tables_exist, connection_test]')
	sys.exit(1)
elif len(sys.argv) > 1: # at least 1 parms
	action = sys.argv[1]


if action.lower() == 'check_tables_exist':
	createtables(zetl_tables_required)

elif action.lower() == 'connection_test':
	try:
		zetldb.connect()
		print(zetldb.queryone('SELECT version()'))
		zetldb.close()
		sys.exit(0)

	except Exception as e:
		print('zetl_initdb.py connection_test  > failed. Check database connection detail variables in setup.bat \n' + str(e))
		sys.exit(1)

else:
	raise Exception('This program needs 1 argument.  check_tables_exist')
	sys.exit(1)


sys.exit(0)

