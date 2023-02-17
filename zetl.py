"""
  Dave Skura, Dec,2022
"""
from zetldb_postgres import zetldb
from postgresdave_package.postgresdave import db #install pip install postgresdave-package

import psycopg2 

import warnings
import sys
import os
import re
from datetime import *
now = (datetime.now())
sztoday=str(now.year) + '-' + ('0' + str(now.month))[-2:] + '-' + str(now.day)

force = True

zetldb = zetldb()

def logstepstart(etl_name,stepnum,cmdfile,steptablename,query,ipart):

	zsql = "INSERT INTO " + zetldb.db.db_conn_dets.DB_SCHEMA + ".z_log (etl_name,dbuser,stepnum,cmdfile,steptablename,"
	zsql += "cmd_to_run,part,rundtm) VALUES ('" + etl_name + "',(SELECT current_user),"
	zsql += str(stepnum) + ",'" + str(cmdfile) + "','" + steptablename + "','" 
	zsql += query.replace('?','').replace("'","`") + "'," + str(ipart) + ", CURRENT_TIMESTAMP);"
	zetldb.db.execute(zsql)
	
	lid = zetldb.db.queryone("SELECT max(id) FROM " + zetldb.db.db_conn_dets.DB_SCHEMA + ".z_log ")
	return lid

def logstepend(lid,the_rowcount,consoleoutput='not-passed-in'):
	usql = "UPDATE  " + zetldb.db.db_conn_dets.DB_SCHEMA + ".z_log SET "

	if consoleoutput!='not-passed-in':
		usql += "script_output='" + consoleoutput.replace("'",'`') + "',"

	usql += "rowcount = " + str(the_rowcount) + ", endtime = CURRENT_TIMESTAMP WHERE id = " + str(lid) 

	try:
		zetldb.db.execute(usql)
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

def log_script_error(lid,script_error,script_output=''):

	usql = "UPDATE  " + zetldb.db.db_conn_dets.DB_SCHEMA + ".z_log SET script_output = '" + script_output.replace("'","`") + "', script_error = '" + script_error.replace("'","`") + "', endtime = CURRENT_TIMESTAMP WHERE id = " + str(lid) 
	try:
		zetldb.db.execute(usql)
		zetldb.db.commit()
	except Exception as e:
		print(str(e))

def run_one_etl_step(etl_name,stepnum,steptablename,cmdfile):

	script_variables = {'DB_USERNAME':'','DB_USERPWD':'','DB_HOST':'','DB_PORT':'','DB_NAME':'','DB_SCHEMA':''}

	findcmdfile = '.\\zetl_scripts\\' + etl_name + '\\' + cmdfile
	try:
		f = open(findcmdfile,'r') 
		sqlfromfile = f.read()

		f.close()
	except Exception as e:
		raise Exception('cannot open cmd file ' + cmdfile)
		print(str(e))
		sys.exit(0)

	sqllines = sqlfromfile.split('\n')
	for i in range(0,len(sqllines)):
		variable_name = sqllines[i].split('=')[0].strip()
		if (variable_name in script_variables):
			script_variables[variable_name] = sqllines[i].split('=')[1].strip()

	sql = RemoveComments(sqlfromfile.strip())

	ipart = 0
	newdb = db()

	for individual_query in sql.split(';'):

		ipart += 1
		individual_query = individual_query.strip()
		if not individual_query.isspace() and individual_query != '':
			script_output = ''

			print('\n file ' + cmdfile + ', step ' + str(ipart))
			print(individual_query)
			#script_output += individual_query + '\n \n'

			lid = logstepstart(etl_name,stepnum,cmdfile,steptablename,individual_query,ipart)

			try:
				if script_variables['DB_USERNAME'] != '': # dont use default connection
					newdb.useConnectionDetails(script_variables['DB_USERNAME']
										,script_variables['DB_USERPWD']
										,script_variables['DB_HOST']
										,script_variables['DB_PORT']
										,script_variables['DB_NAME']
										,script_variables['DB_SCHEMA'])

					if individual_query.strip().upper().find('SELECT') == 0:
						results = newdb.export_query_to_str(individual_query)
						script_output += results
						print('\n' + results)

					else:

						newdb.execute(individual_query)
						newdb.commit()

					logend_steptable(newdb,lid,script_variables,steptablename,script_output)
				else: # use default connection
					if individual_query.strip().upper().find('SELECT') == 0:
						results = zetldb.db.export_query_to_str(individual_query)
						print('\n' + results)
						script_output += results

					else:

						zetldb.db.execute(individual_query)

					logend_steptable(zetldb,lid,script_variables,steptablename,script_output)
			except Exception as e:
				log_script_error(lid,str(e),script_output)
				print(str(e))
				sys.exit(1)


def logend_steptable(dbconn,lid,script_variables,steptablename,script_output):
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
			this_schema = zetldb.db.db_conn_dets.DB_SCHEMA
			this_table = steptablename.split('.')[0]

	qualified_table = this_schema + '.' + this_table
	tblrowcount = 0
	if dbconn.does_table_exist(qualified_table):
		tblrowcount = dbconn.queryone("SELECT COUNT(*) FROM " + qualified_table)
		dbconn.close()

	logstepend(lid,tblrowcount,script_output)


def get_current_activity():
	sql = """
		SELECT *
		FROM (
				SELECT currently,activity_type FROM """ + zetldb.db.db_conn_dets.DB_SCHEMA + """.z_activity
				UNION
				SELECT '' as currently,'default' as activity_type
				) L
		ORDER BY 1 desc
	"""
	data = zetldb.db.query(sql)

	if data[0][1] == 'default':
		return_value = 'idle'
	else:
		return_value = data[0][0] 

	return return_value

def runetl(etl_name):
	tempfilename = 'zetl_pythonscript_temp.log'
	try:
		os.remove(tempfilename)
	except:
		pass
	sql = """
	SELECT stepnum,steptablename,cmdfile 
	FROM """ + zetldb.db.db_conn_dets.DB_SCHEMA + """.z_etl 
	WHERE etl_name = '""" + etl_name + """'
	ORDER BY etl_name, stepnum
	"""
	#print(sql)
	data = zetldb.db.query(sql)
	for row in data:
		stepnum = row[0]
		steptablename = row[1]
		cmdfile = row[2]
		foundfile = '.\\zetl_scripts\\' + etl_name + '\\' + cmdfile
		#print('stepnum = \t\t' + str(stepnum))
		#print('steptablename = \t' + steptablename)
		#print('cmdfile = \t\t' + cmdfile)
		if cmdfile.lower().endswith('.sql') or cmdfile.lower().endswith('.ddl'):
			run_one_etl_step(etl_name,stepnum,steptablename,cmdfile)

		elif cmdfile.lower().endswith('.py'):

			lid = logstepstart(etl_name,stepnum,cmdfile,steptablename,'Python script',0)

			print('\n file ' + cmdfile + '\n')

			exit_code = os.system('py ' + foundfile  + ' > ' + tempfilename)

			f = open(tempfilename,'r')
			consoleoutput = f.read()
			f.close()

			if exit_code != 0:
				print(consoleoutput + '\n exit_code=' + str(exit_code))
				log_script_error(lid,foundfile + ' failed with error_code ' + str(exit_code),consoleoutput.strip())
				logstepend(lid,0)

				sys.exit(exit_code)

			print(consoleoutput)
			logstepend(lid,0,consoleoutput.strip())

		elif cmdfile.lower().endswith('.bat'):
			lid = logstepstart(etl_name,stepnum,cmdfile,steptablename,'Windows bat script',0)

			print('\n file ' + cmdfile + '\n')
			exit_code = os.system(foundfile + ' > ' + tempfilename)
			f = open(tempfilename,'r')
			consoleoutput = f.read()
			f.close()
			
			if exit_code != 0:
				print(consoleoutput + '\n exit_code=' + str(exit_code))
				log_script_error(lid,foundfile + ' failed with error_code ' + str(exit_code),consoleoutput.strip())
				logstepend(lid,0)

				sys.exit(exit_code)

			print(consoleoutput)
			logstepend(lid,0,consoleoutput.strip())

def show_etl_name_list():
	data = zetldb.db.query('SELECT distinct etl_name from ' + zetldb.db.db_conn_dets.DB_SCHEMA + '.z_etl order by etl_name')
	for row in data:
		print(' ' + row[0])

if len(sys.argv) == 1 or sys.argv[1] == 'zetl.py': # no parameters
	print('usage: ')
	print('zetl.py [etl_name] ') 
	print('')
	zetldb.empty_zetl()						# empty the master zetl table
	zetldb.load_folders_to_zetl() # load master zetl table from folders
	#zetldb.export_zetl() # exporting from the database to csv file.
	show_etl_name_list()

else: # run the etl match the etl_name in the etl table
	etl_name_to_run = sys.argv[1]
	run_options=''
	run_description = ''
	if len(sys.argv) == 3:
		run_options = sys.argv[2]
		if run_options == '-f':
			run_description = '  But first, overwriting table ' + zetldb.db.db_conn_dets.DB_SCHEMA + '.z_etl with file zetl_scripts\\' + etl_name_to_run + '\\z_etl.csv'

	print('Running ' + etl_name_to_run + '.' + run_description)

	zetldb.load_folders_to_zetl(etl_name_to_run)
	if run_options == '-f':
		zetldb.load_z_etlcsv_if_forced(etl_name_to_run,run_options)
	else:
		zetldb.export_zetl()

	activity = get_current_activity()
	if activity == 'idle' or force:
		zetldb.db.execute('DELETE FROM ' + zetldb.db.db_conn_dets.DB_SCHEMA + '.z_activity')
		zetldb.db.execute("INSERT INTO " + zetldb.db.db_conn_dets.DB_SCHEMA + ".z_activity(currently,previously) VALUES ('Running " + etl_name_to_run + "','" + activity + "')")

		runetl(etl_name_to_run)

		zetldb.db.execute("UPDATE " + zetldb.db.db_conn_dets.DB_SCHEMA + ".z_activity SET currently = 'idle',previously='Running " + etl_name_to_run + "'")
		zetldb.db.commit()

	else:
		print("zetl is currently busy with '" + activity + "'")


sys.exit(0)


		

