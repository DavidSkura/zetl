"""
  Dave Skura, 2021
  
  File Description:
"""
VERSION = 3.0 # MySQL

import psycopg2 
import sys

from postgresdatabase import db
from zetl_config import variables
from zetl_config import dbinfo

from zetl_modules.dbxfer import fast_import

functions = []
tables_required_huge = []

tables_required_regular = ['_zetl.z_etl','_zetl.z_log','_zetl.z_etl_dependencies','_zetl.z_control','_zetl.etl_running','_zetl.activity']
mydb = db()

def dbconnect():
	mydb.connect()
	return mydb.db

def create_functions():
	dbconn = dbconnect()
	cur = dbconn.cursor()
	try:
		for i in range(0,len(functions)):
			print('creating function: ' + functions[i])
		
			f = open(mydb.dir_install_ddl + '\\functions.ddl','r')
			sql = f.read()
			f.close()
			cur.execute(sql)
			dbconn.commit()

		dbconn.close()

		print('functions created.')
	except Exception as e:
		print('errors creating funcions.' + str(e))

def createviews():
	dbconn = dbconnect()
	cur = dbconn.cursor()
	try:
		f = open(mydb.dir_install_ddl + '\\views.ddl','r')
		sql = f.read()
		f.close()
		cur.execute(sql)
		dbconn.commit()
		dbconn.close()

		print('views in views.ddl created.')
	except:
		print('no views.ddl file found')


def	checktables(tables_list):
	dbconn = dbconnect()
	#mydb.db
	cur = dbconn.cursor()
	for i in range(0,len(tables_list)):
		print('Looking for table: ' + tables_list[i])
		sql = """
			SELECT count(*) 
			FROM information_schema.tables
			WHERE concat(TABLE_SCHEMA,'.',TABLE_NAME) = '""" + tables_list[i] + "'" 
		#print(sql)
		cur.execute(sql)
		table_count = cur.fetchall()[0][0]
		if table_count == 0:
			DDLFile = variables().dir_install_ddl + '\\' + tables_list[i] + '.ddl'
			print(tables_list[i] + ' Not Found.  Creating from ' + DDLFile)
			fh = open(DDLFile,'r')
			sql = fh.read()
			fh.close()
			cur.execute(sql)
			dbconn.commit()
			print(tables_list[i] + ' Created. ')
			try:
				csvfile = 'data//initial_loads//' + tables_list[i] + '.csv'
				fc = open(csvfile,'r')
				fc.close()
				print('initial load found in zetl_install/data/initial_loads')
				fast_import().import_file_to_table(csvfile,tables_list[i])

				cur.execute("SELECT count(*) FROM " + tables_list[i])
				print(tables_list[i] + ' loaded with ' + str(cur.fetchall()[0][0]) + ' rows.')

			except:
				print('No initial load file found in zetl_install/data/initial_loads for ' + tables_list[i])
				pass

		else:
			cur.execute("SELECT count(*) FROM " + tables_list[i])
			print(tables_list[i] + ' Found with ' + str(cur.fetchall()[0][0]) + ' rows.')
				
	dbconn.close()

if len(sys.argv) == 1 or sys.argv[1] == 'zetl.py':
	print('nothing to do')
	sys.exit()

elif len(sys.argv) > 1: # at least 1 parms
	action = sys.argv[1]

if action.lower() == 'connect':

	dbconn = dbconnect()
	print('Connected: ' + mydb.connection_str)
	dbconn.close()

elif action.lower() == 'check_tables_exist':
	checktables(tables_required_regular)

elif action.lower() == 'check_huge_tables_exist':
	checktables(tables_required_huge)

elif action.lower() == 'create_views':
	createviews()

elif action.lower() == 'create_functions':
	create_functions()

else:
	print('nothing to do')