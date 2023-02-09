"""
  Dave Skura, Dec,2022
"""
from zetl_utility_functions import zetlfn
from postgresdatabase import db

import psycopg2 

import warnings
import sys
import os
import re
from datetime import *
now = (datetime.now())
sztoday=str(now.year) + '-' + ('0' + str(now.month))[-2:] + '-' + str(now.day)

force = True

zetldb = db()

def logstepstart(etl_name,stepnum,sqlfile,steptablename,query,ipart):

	zsql = "INSERT INTO " + zetldb.ischema + ".z_log (etl_name,dbuser,stepnum,sqlfile,steptablename,"
	zsql += "sql_to_run,part,rundtm) VALUES ('" + etl_name + "',(SELECT current_user),"
	zsql += str(stepnum) + ",'" + str(sqlfile) + "','" + steptablename + "','" 
	zsql += query.replace('?','').replace("'","`") + "'," + str(ipart) + ", CURRENT_TIMESTAMP);"
	zetldb.execute(zsql)
	
	lid = zetldb.queryone("SELECT max(id) FROM " + zetldb.ischema + ".z_log ")
	return lid

def logstepend(lid,the_rowcount):
	
	usql = "UPDATE  " + zetldb.ischema + ".z_log SET rowcount = " + str(the_rowcount) + ", endtime = CURRENT_TIMESTAMP WHERE id = " + str(lid) 
	try:
		zetldb.execute(usql)
	except Exception as e:
		print(str(e))
		sys.exit(1) 


def f1(foo=''): return iter(foo.splitlines())

def RemoveComments(asql):
	foundacommentstart = 0
	foundacommentend = 0
	ret = ""

	for line in f1(asql):
		
		if not line.startswith( '--' ):
			if line.find('/*') > -1:
				foundacommentstart += 1

			if line.find('*/') > -1:
				foundacommentend += 1
			
			if foundacommentstart == 0:
				ret += line + '\n'

			if foundacommentstart > 0 and foundacommentend > 0:
				foundacommentstart = 0
				foundacommentend = 0	

	return ret

def log_sql_error(lid,sql_error):

	usql = "UPDATE  " + zetldb.ischema + ".z_log SET sql_error = '" + sql_error.replace("'","`") + "', endtime = CURRENT_TIMESTAMP WHERE id = " + str(lid) 
	try:
		zetldb.execute(usql)
	except Exception as e:
		print(str(e))

def run_one_etl_step(etl_name,stepnum,steptablename,sqlfile):

	script_variables = {'DB_USERNAME':'','DB_USERPWD':'','DB_HOST':'','DB_PORT':'','DB_NAME':'','DB_SCHEMA':''}

	findsqlfile = '.\\zetl_scripts\\' + etl_name + '\\' + sqlfile
	try:
		f = open(findsqlfile,'r') 
		sqlfromfile = f.read()

		f.close()
	except Exception as e:
		raise Exception('cannot open sql file ' + sqlfile)
		print(str(e))
		sys.exit(0)

	sqllines = sqlfromfile.split('\n')
	for i in range(0,len(sqllines)):
		variable_name = sqllines[i].split('=')[0].strip()
		if (variable_name in script_variables):
			script_variables[variable_name] = sqllines[i].split('=')[1].strip()

	sql = RemoveComments(sqlfromfile.strip())

	ipart = 0
	for individual_query in sql.split(';'):
		newdb = db()

		ipart += 1
		individual_query = individual_query.strip()
		if not individual_query.isspace() and individual_query != '':
			print('\nin file ' + sqlfile + ', step ' + str(ipart))
			print(individual_query)

			lid = logstepstart(etl_name,stepnum,sqlfile,steptablename,individual_query,ipart)

			try:
				if script_variables['DB_USERNAME'] != '': # dont use default connection
					newdb.setvars(script_variables['DB_USERNAME'],
										script_variables['DB_USERPWD'],
										script_variables['DB_HOST'],
										script_variables['DB_PORT'],
										script_variables['DB_NAME'],
										script_variables['DB_SCHEMA'])
				
					if individual_query.strip().upper().find('SELECT') == 0: 
						print(newdb.export_query_to_string(individual_query))
					else:
						newdb.execute(individual_query)
						newdb.commit()
				else: # use default connection
					if individual_query.strip().upper().find('SELECT') == 0: 
						print(zetldb.export_query_to_string(individual_query))
					else:
						zetldb.execute(individual_query)
						zetldb.commit()
				

			except Exception as e:
				log_sql_error(lid,str(e))

			if script_variables['DB_USERNAME'] != '': # dont use default connection
				try:
					this_table = steptablename.split('.')[1]
					this_schema = steptablename.split('.')[0]
				except:
					if script_variables['DB_SCHEMA'] !='':
						this_schema = script_variables['DB_SCHEMA']
					else:
						this_schema = 'public'
					this_table = steptablename
			else: # use default connection
				this_schema = steptablename.split('.')[0]
				try:
					this_table = steptablename.split('.')[1]
				except:
					this_schema = zetldb.ischema
					this_table = steptablename.split('.')[0]

			qualified_table = this_schema + '.' + this_table
			
			if script_variables['DB_USERNAME'] != '': # dont use default connection
				if newdb.does_table_exist(qualified_table):
					tblrowcount = newdb.queryone("SELECT COUNT(*) FROM " + qualified_table)
					newdb.close()
					logstepend(lid,tblrowcount)

			else:# use default connection
				if zetldb.does_table_exist(qualified_table):

					tblrowcount = zetldb.queryone("SELECT COUNT(*) FROM " + qualified_table)
					logstepend(lid,tblrowcount)

def get_current_activity():
	sql = """
		SELECT *
		FROM (
				SELECT currently,activity_type FROM """ + zetldb.ischema + """.z_activity
				UNION
				SELECT '' as currently,'default' as activity_type
				) L
		ORDER BY 1 desc
	"""
	data = zetldb.query(sql)

	if data[0][1] == 'default':
		return_value = 'idle'
	else:
		return_value = data[0][0] 

	return return_value

def runetl(etl_name):
	sql = """
	SELECT stepnum,steptablename,sqlfile 
	FROM """ + zetldb.ischema + """.z_etl 
	WHERE etl_name = '""" + etl_name + """'
	ORDER BY etl_name, stepnum
	"""
	#print(sql)
	data = zetldb.query(sql)
	for row in data:
		stepnum = row[0]
		steptablename = row[1]
		sqlfile = row[2]
		#print('stepnum = \t\t' + str(stepnum))
		#print('steptablename = \t' + steptablename)
		#print('sqlfile = \t\t' + sqlfile)
		run_one_etl_step(etl_name,stepnum,steptablename,sqlfile)

 
def show_etl_name_list():
	data = zetldb.query('SELECT distinct etl_name from ' + zetldb.ischema + '.z_etl order by etl_name')
	for row in data:
		print(' ' + row[0])

if len(sys.argv) == 1 or sys.argv[1] == 'zetl.py' or sys.argv[1] == '-fa': # no parameters
	print('zetl v2.1\n usage: ')
	print('zetl.py [etl_name] [-f] [-fa]') # -f will cause the csv file in the etl path to overwrite the z_etl table.
	# -fa will empty zetl table and refresh from folders and files
	print('')
	if sys.argv[1] == '-fa':
		print( 'truncating table ' + zetldb.ischema + '.z_etl')
		zetldb.execute('TRUNCATE TABLE ' + zetldb.ischema + '.z_etl')

	zetldb.load_folders_to_zetl()
	zetldb.export_zetl()
	show_etl_name_list()

else: # run the etl match the etl_name in the etl table
	etl_name_to_run = sys.argv[1]
	run_options=''
	run_description = ''
	if len(sys.argv) == 3:
		run_options = sys.argv[2]
		if run_options == '-f':
			run_description = '  But first, overwriting table ' + zetldb.ischema + '.z_etl with file zetl_scripts\\' + etl_name_to_run + '\\z_etl.csv'

	print('Running ' + etl_name_to_run + '.' + run_description)

	zetldb.load_folders_to_zetl(etl_name_to_run)
	if run_options == '-f':
		zetldb.load_z_etlcsv_if_forced(etl_name_to_run,run_options)
	else:
		zetldb.export_zetl()

	activity = get_current_activity()
	if activity == 'idle' or force:
		zetldb.execute('DELETE FROM ' + zetldb.ischema + '.z_activity')
		zetldb.execute("INSERT INTO " + zetldb.ischema + ".z_activity(currently,previously) VALUES ('Running " + etl_name_to_run + "','" + activity + "')")

		runetl(etl_name_to_run)

		zetldb.execute("UPDATE " + zetldb.ischema + ".z_activity SET currently = 'idle',previously='Running " + etl_name_to_run + "'")
		zetldb.dbconn.commit()

	else:
		print("zetl is currently busy with '" + activity + "'")


sys.exit(0)


		

