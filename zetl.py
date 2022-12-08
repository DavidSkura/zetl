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

			lid = logstepstart(etl_name,stepnum,sqlfile,steptablename,individual_query,ipart)

			zetldb.execute(individual_query)
			if zetldb.does_table_exist(steptablename):
				this_schema = steptablename.split('.')[0]
				try:
					this_table = steptablename.split('.')[1]
				except:
					this_schema = self.ischema
					this_table = steptablename.split('.')[0]
				qualified_table = this_schema + '.' + this_table

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
	if activity == 'idle' or force:
		zetldb.execute('DELETE FROM ' + zetldb.ischema + '.z_activity')
		zetldb.execute("INSERT INTO " + zetldb.ischema + ".z_activity(currently,previously) VALUES ('Running " + etl_name_to_run + "','" + activity + "')")

		runetl(etl_name_to_run)

		zetldb.execute("UPDATE " + zetldb.ischema + ".z_activity SET currently = 'idle',previously='Running " + etl_name_to_run + "'")
		zetldb.dbconn.commit()

	else:
		print("zetl is currently busy with '" + activity + "'")


sys.exit(0)


		

