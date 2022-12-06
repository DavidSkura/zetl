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
	runetl(etl_name_to_run)

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
		


def compose_sql(sqljob):
	isql = 'DROP TABLE IF EXISTS ' + sqljob.steptablename + ';\n'
	isql += 'CREATE TABLE ' + sqljob.steptablename + ' AS \n'
	isql += 'SELECT '
	if not sqljob.groupbyfield1 is None: isql += '\t' + str(sqljob.groupbyfield1) + ', '
	if not sqljob.groupbyfield2 is None: isql += str(sqljob.groupbyfield2) + ', '
	if not sqljob.groupbyfield3 is None: isql += str(sqljob.groupbyfield3) + ', '
	if not sqljob.groupbyfield4 is None: isql += str(sqljob.groupbyfield4) + ', '
	if not sqljob.groupbyfield5 is None: isql += str(sqljob.groupbyfield5) + ', '

	if not sqljob.aggfield1 is None: isql +=  str(sqljob.aggfield1)  + ', '
	if not sqljob.aggfield2 is None: isql +=  str(sqljob.aggfield2)  + ', '
	if not sqljob.aggfield3 is None: isql +=  str(sqljob.aggfield3)  + ', '
	if not sqljob.aggfield4 is None: isql +=  str(sqljob.aggfield4)  + ', '
	if not sqljob.aggfield5 is None: isql +=  str(sqljob.aggfield5)  + ', '
 
	if not sqljob.countfield1 is None: isql +=  str(sqljob.countfield1)  + ', '
	if not sqljob.countfield2 is None: isql +=  str(sqljob.countfield2)  + ', '
	if not sqljob.countfield3 is None: isql +=  str(sqljob.countfield3)  + ', '
	if not sqljob.countfield4 is None: isql +=  str(sqljob.countfield4)  + ', '
	if not sqljob.countfield5 is None: isql +=  str(sqljob.countfield5)  + ', '
	isql = isql.strip()[:-1]

	isql += '\nFROM \n'
	if not sqljob.innerjoin1 is None: isql += '\t' + str(sqljob.innerjoin1) + '\n'
	if not sqljob.innerjoin2 is None: isql += '\t' + str(sqljob.innerjoin2) + '\n'
	if not sqljob.innerjoin3 is None: isql += '\t' + str(sqljob.innerjoin3) + '\n'
	if not sqljob.innerjoin4 is None: isql += '\t' + str(sqljob.innerjoin4) + '\n'
	if not sqljob.innerjoin5 is None: isql += '\t' + str(sqljob.innerjoin5) + '\n'
	if not sqljob.leftjoin1 is None: isql += '\t' + str(sqljob.leftjoin1) + '\n'
	if not sqljob.leftjoin2 is None: isql += '\t' + str(sqljob.leftjoin2) + '\n'
	if not sqljob.leftjoin3 is None: isql += '\t' + str(sqljob.leftjoin3) + '\n'
	if not sqljob.leftjoin4 is None: isql += '\t' + str(sqljob.leftjoin4) + '\n'
	if not sqljob.leftjoin5 is None: isql += '\t' + str(sqljob.leftjoin5) + '\n'
	if not sqljob.whereclause is None: isql += '\t' + str(sqljob.whereclause) + '\n'

	if not sqljob.groupbyfield1 is None:
		isql += '\nGROUP BY ' + str(sqljob.groupbyfield1) + ', '
		if not sqljob.groupbyfield2 is None: isql += str(sqljob.groupbyfield2) + ', '
		if not sqljob.groupbyfield3 is None: isql += str(sqljob.groupbyfield3) + ', '
		if not sqljob.groupbyfield4 is None: isql += str(sqljob.groupbyfield4) + ', '
		if not sqljob.groupbyfield5 is None: isql += str(sqljob.groupbyfield5) + ', '
		isql = isql.strip()[:-1]

	isql += ';'

	return isql

def run_one_etl_step(etl_job,etl_name,stepnum,rundtm,output_html):

	found_step_at_i = -1
	for i in range(1,len(etl_job.steps)):
		if etl_job.steps[i].stepnum == stepnum:
			found_step_at_i = i
		
	if found_step_at_i == -1:
		print('step not found in ' + etl_name)
		sys.exit(1)
	else:
		job_to_run = etl_job.steps[found_step_at_i]

	runit(etl_job,job_to_run,rundtm,output_html)
	

x.load_etl_job_details(Arg_etl_name_or_num)
if x.loaded==0:
	sys.exit(1)
x.setstock(stock)
x.setparam(argv2,argv3)

rundtm = Query(x.cur,'SELECT CURRENT_TIMESTAMP')

dbstatus = Query(x.cur,'SELECT currently FROM _zetl.activity')
dbcurkeyfld = Query(x.cur,'SELECT keyfld FROM _zetl.activity')
if ((dbstatus.find('*hold') > -1) and (force_run==False)):
	print(' currently ' + dbstatus + ' in Activity.  Not doing anything else right now.')
else:
	noheader=0
	if len(x.steps) > 1:
		if x.steps[1].note is not None: 
			if x.steps[1].note.lower().strip().find('no header') > -1:
				noheader=1

	if noheader == 0:
		print('Running etl ' + Arg_etl_name_or_num + ', for user ' +  os.getenv('username', 'not found') + ', at ' + str(rundtm))

	runetl(x,Arg_etl_name_or_num,rundtm,output_html)

	if x.out_to_file:
		f = open(x.sz_out_filename,'r')
		data = f.read()
		f.close()
		print(data)
	
	dbpreviousstatus = Query(x.cur,'SELECT currently FROM _zetl.activity')
	dbprevkeyfld = Query(x.cur,'SELECT keyfld FROM _zetl.activity')

	uSQL="UPDATE _zetl.activity SET currently = '" + dbstatus + "', previously='" + dbpreviousstatus + "',keyfld='" + dbcurkeyfld + "',prvkeyfld='" + dbprevkeyfld + "',dtm=CURRENT_TIMESTAMP"
	x.cur.execute(uSQL)

	x.cur.close()
	#x.dbconn.close()

sys.exit(0)


