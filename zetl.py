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
variable_dictionary	= {}

zetldb = db()

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

def run_one_etl_step(etl_name,stepnum,steptablename,sqlfile):
	findsqlfile = '.\\zetl_scripts\\demo\\' + sqlfile
	try:
		f = open(findsqlfile,'r') 
		sqlfromfile = f.read()
		f.close()
	except Exception as e:
		raise Exception('cannot open sql file ' + sqlfile)
		print(str(e))
		sys.exit(0)

	sql = RemoveComments(sqlfromfile.strip())

	ipart = 0
	for individual_query in sql.split(';'):
		ipart += 1
		individual_query = individual_query.strip()
		if not individual_query.isspace() and individual_query != '':
			print('\nin file ' + sqlfile + ', step ' + str(ipart))
			print(individual_query)
			zetldb.execute(individual_query)

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

if len(sys.argv) == 1 or sys.argv[1] == 'zetl.py':
	if not zetldb.check_etl_names(): # false means no rows.
		print("zetl.py accepts parameters such as an etl_name found in '" + zetldb.idb + '.' + zetldb.ischema + ".z_etl'")

	data = zetldb.query('SELECT distinct etl_name from ' + zetldb.ischema + '.z_etl order by etl_name')
	for row in data:
		print(' ' + row[0])
	sys.exit()

else: # run the etl match the etl_name in the etl table
	etl_name_to_run = sys.argv[1]
	print('Running ' + etl_name_to_run)
	activity = get_current_activity()
	if activity == 'idle':
		zetldb.execute('DELETE FROM ' + zetldb.ischema + '.z_activity')
		zetldb.execute("INSERT INTO " + zetldb.ischema + ".z_activity(currently,previously) VALUES ('Running " + etl_name_to_run + "','" + activity + "')")

		runetl(etl_name_to_run)

		zetldb.execute("UPDATE " + zetldb.ischema + ".z_activity SET currently = 'idle',previously='Running " + etl_name_to_run + "'")
		zetldb.dbconn.commit()

	else:
		print("zetl is currently busy with '" + activity + "'")


sys.exit(0)


def logstepstart(cur,job_to_run,query,ipart,rundtm,sqltype):

	zsql = "INSERT INTO _zetl.z_log (etl_name,dbuser,stepnum,sqlfile,steptablename,"
	zsql += "table_or_view,line,sql_to_run,part,rundtm) VALUES ('" + job_to_run.etl_name + "',(SELECT current_user),"
	zsql += str(job_to_run.stepnum) + ",'" + str(job_to_run.sqlfile).replace("\\","\\\\") + "','" + job_to_run.steptablename + "','" 
	zsql += job_to_run.table_or_view + "','" + sqltype + "','"
	zsql += query.replace('?','').replace("'","`") + "'," + str(ipart) + ", '" + str(rundtm) + "');"
	Query(cur,zsql)
	
	usql = "SELECT max(id) FROM _zetl.z_log WHERE endtime is null and etl_name = '" + job_to_run.etl_name + "' and stepnum= " + str(job_to_run.stepnum)
	lid = Query(cur,usql)	

	return lid

def logstepend(cur,job_to_run,query,lid):
	
	usql = "UPDATE _zetl.z_log SET endtime = CURRENT_TIMESTAMP WHERE id = " + str(lid) 
	try:
		Query(cur,usql)
	except Exception as e:
		print(str(e))
		sys.exit(1) 
		

