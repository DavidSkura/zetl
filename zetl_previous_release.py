"""
  Dave Skura, May 3,2019
"""
VERSION=191029
import mysql.connector
from mysqldatabase import db
from zetl_config import variables
from zetl_config import dbinfo

import warnings
import sys
import os
import re
from etl_classes import etl_step, etl_job
from DaveMath import davemath,resultset
from datetime import *
now = (datetime.now())

sztoday=str(now.year) + '-' + ('0' + str(now.month))[-2:] + '-' + str(now.day)

x = etl_job()


if len(sys.argv) == 1 or sys.argv[1] == 'zetl.py':
	print("\nUsage: zetl.py <etl_name>")
	print("	eg. zetl.py main")
	print("	")
	print("to run during *hold* do this: \n")
	print("zetl.py <etl_name> -f ")
	print("	")
	print("for html output do this: \n")
	print("zetl.py <etl_name> html ")

	RowsReturned = x.cur.execute('SELECT distinct etl_name from _zetl.z_etl order by etl_name')
	data=x.cur.fetchall()
	for row in data:
		print(' ' + row[0])
	sys.exit()

force_run = False
argv3 = ''
Arg_etl_name_or_num = sys.argv[1]
output_html = False
if len(sys.argv) > 2: # at least 2 parms
	argv2 = sys.argv[2]
	output_html = False
	if argv2.lower().strip() == 'html':
		output_html = True
		# generate html
		if len(sys.argv) > 3: 
			argv3 = sys.argv[3]
	elif argv2.lower().strip() == '-f':
		force_run = True

	else:
		if len(sys.argv) > 3: # if there is a third parm it's a date
			argv3 = sys.argv[3]
		else:					# no third parm
			if len(argv2) < 7:  # if second parm is a stock...
				argv3 = sztoday # set third parm to today
			else:
				argv3 = argv3	# else just set it to the same value as 2nd parm

else:
	argv2 = sztoday
	argv3 = sztoday

stock = argv2
datetorun = argv3

variable_dictionary	= {}

foo = ""
def f1(foo=foo): return iter(foo.splitlines())

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

def Query(zCur, zsql):
	RowsReturned = zCur.execute(zsql)
	R = ""
	try:
		data=zCur.fetchall()
		R = data[0][0]
	except Exception: 
		pass

	return R

def f1(foo=foo): return iter(foo.splitlines())

def setvars(multicommandblock):
	ret = ""

	for line in f1(multicommandblock):
		if line.upper().startswith('\SET '):
			for k, v in variable_dictionary.items():
				line = line.replace(':'+k,v)

			setvar = line.split(' ')
			varname = setvar[1]
			varvalue = line[len('\set ') + len(setvar[1]) + 1:].strip()
			
			#if varvalue[:1] == varvalue[-1:] and len(varvalue) > 2:
			#	varvalue = varvalue[1:-1]

			variable_dictionary[varname] = str(varvalue)

		else:
			ret += line + '\n'

	return ret

def does_table_exist(zCur,tblname):
	csql = "SELECT count(*) FROM INFORMATION_SCHEMA.tables WHERE lower(table_name) = lower('" + tblname + "')"
	ichk_table_exists = Query(zCur,csql)
	if ichk_table_exists == 0:
		return False
	else:
		return True


def runit(etl_job, jobtorun,rundtm,output_html):
	zCur = etl_job.cur
	
	if not (jobtorun.sqlfile is None or jobtorun.sqlfile == ''):
		etl_job.setsqlfile(jobtorun.sqlfile.split('\\')[len(jobtorun.sqlfile.split('\\'))-1])
		try:
			f = open(jobtorun.sqlfile,'r') 
			jobtorun.sql = f.read()
			f.close()
		except Exception as e:
			print('cannot open sql file ' + jobtorun.sqlfile)
			print(str(e))
			sys.exit(0)
	else:
		etl_job.setsqlfile('no_sql_file')

	ipart = -1
	if not jobtorun.sql is None and jobtorun.sql != '':
		zsql = jobtorun.sql
		sqltype = 'sql provided'
	else:
		zsql = compose_sql(jobtorun)
		sqltype = 'generated sql'
	
	stock=''
	stgtablename=''
	arg2=''
	arg3=''
	chk_table_exists=''
	variable_dictionary['output_file'] = ''
	variable_dictionary['chk_table_exists'] = ''
	sqlfile = Query(zCur,"SELECT parametervalue FROM _zetl.z_control WHERE parameterkey='sqlfile' and etl_name='zetl'");
	stock = Query(zCur,"SELECT parametervalue FROM _zetl.z_control WHERE parameterkey='stock' and etl_name='zetl'");
	arg2 = Query(zCur,"SELECT parametervalue FROM _zetl.z_control WHERE parameterkey='arg2' and etl_name='zetl'");
	arg3 = Query(zCur,"SELECT parametervalue FROM _zetl.z_control WHERE parameterkey='arg3' and etl_name='zetl'");
	stgtablename = Query(zCur,"SELECT parametervalue FROM _zetl.z_control WHERE parameterkey='stgtablename' and etl_name='zetl'");

	now = (datetime.now())
	variable_dictionary['YYYYMMDD'] = str(now.year) + ('0' + str(now.month))[-2:] + str(now.day) 

	if sqlfile.strip() != '':
		variable_dictionary['sqlfile'] = sqlfile 

	if stock.strip() != '':
		variable_dictionary['stock'] = stock 

	if arg2.strip() != '':
		variable_dictionary['arg2'] = arg2 

	if arg3.strip() != '':
		variable_dictionary['arg3'] = arg3 
	
	#	print(stgtablename)
	# 	sys.exit(0)

	if stgtablename.strip() != '':
		variable_dictionary['stgtablename'] = stgtablename 

	variable_dictionary['etl_name'] = jobtorun.etl_name

	zsql = setvars(zsql) # get vars from file
	
	for k, v in variable_dictionary.items():
		zsql = zsql.replace(':'+k,v)

	Queries = RemoveComments(zsql.strip())

	if variable_dictionary['output_file'].strip() != '':
		x.out_to_file = True
		x.sz_out_filename = variable_dictionary['output_file'].strip()
		fl = open(x.sz_out_filename,'w')
		fl.write('')
		fl.close()


	if variable_dictionary['chk_table_exists'].strip() != '':
		csql = "SELECT count(*) FROM INFORMATION_SCHEMA.tables WHERE lower(table_name) = lower('" + variable_dictionary['chk_table_exists'].strip() + "')"
		ichk_table_exists = Query(zCur,csql)
		if ichk_table_exists == 0:
			print('table does not exist.  Cannot continue.')
			sys.exit(1)
		else:
			print('table exists.')

	for qry in Queries.split(';'):
		ipart += 1
		query = qry.strip()
		if not query.isspace() and query != '':

			lid = logstepstart(zCur,jobtorun,query,ipart,rundtm,sqltype)
			#etl_job.dbconn.commit()

			try:
				warnings.filterwarnings("ignore")
		
				nbr_rows_returned = zCur.execute(query)
				nbr_rows_returned = zCur.rowcount
				if query.strip().lower().find('select') ==0:
					if x.out_to_file:
						resultset(zCur).fileit(x.sz_out_filename)
					else:
						resultset(zCur).printit()


				logstepend(zCur,jobtorun,query,lid)	
				
				if jobtorun.steptablename.strip() != '':
					if does_table_exist(zCur,jobtorun.steptablename):
						sz = 'UPDATE _zetl.z_log SET rowcount=(SELECT COUNT(*) FROM ' + jobtorun.steptablename +') WHERE id = ' + str(lid)
					else:
						sz = 'UPDATE _zetl.z_log SET rowcount=0 WHERE id = ' + str(lid)

					zCur.execute(sz)
				
				sz = "UPDATE _zetl.z_log SET rows_affected='" + str(nbr_rows_returned) + "' WHERE id = " + str(lid)
				zCur.execute(sz)
				#etl_job.dbconn.commit()
	
			except Exception as e:
				print(query)

				ignore_this = 0
				# need to set an onerror condition or threshold handler if a single step fails

				errorline = "run_etl.py:" + jobtorun.etl_name + ":" + str(jobtorun.stepnum) + ":runit:Exception:" + str(e)
				print(errorline)
				zCur.execute("UPDATE _zetl.z_log SET sql_error = '" + str(errorline).replace("'","").replace('?','')[:255] + "' WHERE id = " + str(lid))
				#etl_job.dbconn.commit()
				sys.exit(1) 

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
		

def runetl(etl_job, etl_name,rundtm,output_html):

	order = []
	for i in range(1,len(etl_job.steps)):
		order.append( etl_job.steps[i].stepnum )
	order.sort()

	if order is None:
		print('etl not found')


	for seq_nbr in order:
		# print(' running #' + str(seq_nbr))
		run_one_etl_step(etl_job,etl_name,seq_nbr,rundtm,output_html)
		#etl_job.dbconn.commit()

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


