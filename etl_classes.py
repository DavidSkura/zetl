"""
  Dave Skura, May 3,2019
"""
import psycopg2 
from postgresdatabase import db
import sys
import os
import re
import warnings
from DaveMath import davemath,resultset
from zetl_config import variables
from datetime import *
now = (datetime.now())

foo = ""

class etl_runner:
	def __init__(self):
		self.one_etl_job = etl_job()
		self.argv2 = ''
		self.argv3 = ''
		self.Arg_etl_name_or_num = ''
		self.variable_dictionary	= {}

	def f1(self,foo=foo): return iter(foo.splitlines())

	def RemoveComments(self,asql):
		foundacommentstart = 0
		foundacommentend = 0
		ret = ""

		for line in self.f1(asql):
			
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

	def Query(self,zCur, zsql):
		RowsReturned = zCur.execute(zsql)
		R = ""
		try:
			data=zCur.fetchall()
			R = data[0][0]
		except Exception: 
			pass

		return R


	def setvars(self,multicommandblock):
		ret = ""

		for line in self.f1(multicommandblock):
			if line.upper().startswith('\SET '):
				for k, v in self.variable_dictionary.items():
					line = line.replace(':'+k,v)

				setvar = line.split(' ')
				varname = setvar[1]

				varvalue = line[len('\set ') + len(setvar[1]) + 1:].strip()

				#if varvalue[:1] == varvalue[-1:] and len(varvalue) > 2:
				#	varvalue = varvalue[1:-1]

				self.variable_dictionary[varname] = str(varvalue)

				#if varname == 'chk_table_exists':
				#	print("'varname == 'chk_table_exists'")
				#	print('str(varvalue) = ' + str(varvalue))
				#	print('line = ' + str(line))
				#	sys.exit(1)


			else:
				ret += line + '\n'

		return ret

	def run_one_etl_step(self,etl_job,etl_name,stepnum,rundtm):

		found_step_at_i = -1
		for i in range(1,len(etl_job.steps)):
			if etl_job.steps[i].stepnum == stepnum:
				found_step_at_i = i
			
		if found_step_at_i == -1:
			print('step not found in ' + etl_name)
			sys.exit(1)
		else:
			job_to_run = etl_job.steps[found_step_at_i]

		return self.runit(etl_job,job_to_run,rundtm)
		

	def runit(self,etl_job, jobtorun,rundtm):
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
		self.variable_dictionary['chk_table_exists'] = ''
		sqlfile = self.Query(zCur,"SELECT parametervalue FROM _zetl.z_control WHERE parameterkey='sqlfile' and etl_name='zetl'");
		stock = self.Query(zCur,"SELECT parametervalue FROM _zetl.z_control WHERE parameterkey='stock' and etl_name='zetl'");
		arg2 = self.Query(zCur,"SELECT parametervalue FROM _zetl.z_control WHERE parameterkey='arg2' and etl_name='zetl'");
		arg3 = self.Query(zCur,"SELECT parametervalue FROM _zetl.z_control WHERE parameterkey='arg3' and etl_name='zetl'");
		stgtablename = self.Query(zCur,"SELECT parametervalue FROM _zetl.z_control WHERE parameterkey='stgtablename' and etl_name='zetl'");

		now = (datetime.now())
		self.variable_dictionary['YYYYMMDD'] = str(now.year) + ('0' + str(now.month))[-2:] + str(now.day) 

		if sqlfile.strip() != '':
			self.variable_dictionary['sqlfile'] = sqlfile 

		if stock.strip() != '':
			self.variable_dictionary['stock'] = stock 

		if arg2.strip() != '':
			self.variable_dictionary['arg2'] = arg2 

		if arg3.strip() != '':
			self.variable_dictionary['arg3'] = arg3 

		if stgtablename.strip() != '':
			self.variable_dictionary['stgtablename'] = stgtablename 

		self.variable_dictionary['etl_name'] = jobtorun.etl_name

		zsql = self.setvars(zsql) # get vars from file
		
		for k, v in self.variable_dictionary.items():
			zsql = zsql.replace(':'+k,v)

		Queries = self.RemoveComments(zsql.strip())

		tablefound = True
		#print(self.variable_dictionary)
		#print(self.variable_dictionary['chk_table_exists'].strip())
		#sys.exit(1)
		if self.variable_dictionary['chk_table_exists'].strip() != '':
			csql = "SELECT count(*) FROM INFORMATION_SCHEMA.tables WHERE lower(table_name) = lower('" + self.variable_dictionary['chk_table_exists'].strip() + "')"
			ichk_table_exists = self.Query(zCur,csql)
			if ichk_table_exists == 0:
				print(self.variable_dictionary['chk_table_exists'].strip() + ' table does not exist.  Cannot continue.')
				tablefound = False
			else:
				print(self.variable_dictionary['chk_table_exists'].strip() + ' table exists.')

		if tablefound:
			for qry in Queries.split(';'):
				ipart += 1
				query = qry.strip()
				if not query.isspace() and query != '':

					lid = self.logstepstart(zCur,jobtorun,query,ipart,rundtm,sqltype)
					etl_job.dbconn.commit()

					try:
						warnings.filterwarnings("ignore")
				
						nbr_rows_returned = zCur.execute(query)
						nbr_rows_returned = zCur.rowcount
						if query.strip().lower().find('select') ==0:
							resultset(zCur).printit()

						self.logstepend(zCur,jobtorun,query,lid)	

						if jobtorun.steptablename.strip() != '':
							sz = 'UPDATE _zetl.z_log SET rowcount=(SELECT COUNT(*) FROM ' + jobtorun.steptablename +') WHERE id = ' + str(lid)
			
							zCur.execute(sz)
						
						sz = "UPDATE _zetl.z_log SET rows_affected='" + str(nbr_rows_returned) + "' WHERE id = " + str(lid)
						zCur.execute(sz)
						etl_job.dbconn.commit()
				
					except Exception as e:
						ignore_this = 0
						# need to set an onerror condition or threshold handler if a single step fails

						if str(e).find('1050') > -1 or str(e).find('1146') > -1 :
							#not an issue
							ignore_this = 1
							pass

						else:

							errorline = "run_etl.py:" + jobtorun.etl_name + ":" + str(jobtorun.stepnum) + ":runit:Exception:" + str(e)
							print(errorline)
							zCur.execute("UPDATE _zetl.z_log SET sql_error = '" + str(errorline).replace("'","").replace('?','')[:8000] + "' WHERE id = " + str(lid))
							etl_job.dbconn.commit()
							sys.exit(1) 
		return tablefound


	def logstepstart(self,cur,job_to_run,query,ipart,rundtm,sqltype):

		zsql = "INSERT INTO _zetl.z_log (etl_name,dbuser,stepnum,sqlfile,steptablename,"
		zsql += "table_or_view,line,sql_to_run,part,rundtm) VALUES ('" + job_to_run.etl_name + "',(SELECT current_user),"
		zsql += str(job_to_run.stepnum) + ",'" + str(job_to_run.sqlfile).replace("\\","\\\\") + "','" + job_to_run.steptablename + "','" 
		zsql += job_to_run.table_or_view + "','" + sqltype + "','"
		zsql += query.replace('?','').replace("'","`") + "'," + str(ipart) + ", cast('" + str(rundtm) + "' as timestamp));"
		self.Query(cur,zsql)
		
		usql = "SELECT max(id) FROM _zetl.z_log WHERE endtime is null and etl_name = '" + job_to_run.etl_name + "' and stepnum= " + str(job_to_run.stepnum)
		lid = self.Query(cur,usql)	

		return lid

	def logstepend(self,cur,job_to_run,query,lid):
		
		usql = "UPDATE _zetl.z_log SET endtime = CURRENT_TIMESTAMP WHERE id = " + str(lid) 
		try:
			self.Query(cur,usql)
		except Exception as e:
			print(str(e))
			sys.exit(1) 
		
	def runetl(self,etl_job, etl_name,rundtm):
		order = []
		for i in range(1,len(etl_job.steps)):
			order.append( etl_job.steps[i].stepnum )
		order.sort()

		if order is None:
			print('etl not found')

		for seq_nbr in order:
			# print(' running #' + str(seq_nbr))
			result = self.run_one_etl_step(etl_job,etl_name,seq_nbr,rundtm)
			etl_job.dbconn.commit()
			if result == False:
				return False

		return True

class etl_step:
	"""
	Base class instance for etl_step
	"""
	def __init__(self):
		self.etl_name='unloaded'
		self.stepnum='unloaded'
		self.sql=''
		self.sqlfile=''
		self.steptablename=''
		self.table_or_view=''
		self.estrowcount=''
		self.whereclause=''
		self.note=''
		self.dtm=''

	def __str__(self):
		X = "etl_name = " + str(self.etl_name) + '\n'
		X += "stepnum = " + str(self.stepnum) + '\n'
		if not self.table_or_view is None: X += "table_or_view = " + str(self.table_or_view) + '\n'
		if not self.steptablename is None: X += "steptablename = " + str(self.steptablename) + '\n'
		if not self.sql is None: X += "sql = " + str(self.sql) + '\n'
		if not self.sqlfile is None: X += "sqlfile = " + str(self.sqlfile) + '\n'
		if not self.whereclause is None: X += "whereclause = " + str(self.whereclause) + '\n'
		if not self.note is None: X += "note = " + str(self.note) + '\n'
		if not self.dtm is None: X += "dtm = " + str(self.dtm) + '\n'
		return X	

class etl_job:
	"""
	Base class instance for all etl_step
	"""
	def __init__(self):
		self.steps = [tuple()] # list of etl_step(s)
		self.loaded = 0
		self.out_to_file = False
		self.sz_out_filename = ''
		self.mydb = db()
		sys.stdout.flush()

		try:

			self.dbconn = self.mydb.connect()
			self.cur = self.mydb.cur

		except:
			print (" I am unable to connect to the database")
			exit()

	def setparam(self,arg2,arg3):
		self.arg2 = arg2
		if arg3.strip() == '':
			self.arg3 = self.arg2
		else:
			self.arg3 = arg3

		self.cur.execute("DELETE FROM _zetl.z_control WHERE etl_name='zetl' and parameterkey in ('arg2','arg3')")
		self.cur.execute("INSERT INTO _zetl.z_control (etl_name,parameterkey,parametervalue) VALUES ('zetl','arg2','" + self.arg2 + "')")
		self.cur.execute("INSERT INTO _zetl.z_control (etl_name,parameterkey,parametervalue) VALUES ('zetl','arg3','" + self.arg3 + "')")

	def setstock(self,stock):
		self.stock = stock
		self.stgtablename = stock.replace(".","") + '_stg'
		self.cur.execute("DELETE FROM _zetl.z_control WHERE etl_name='zetl' and parameterkey in ('stock','stgtablename')")
		self.cur.execute("INSERT INTO _zetl.z_control (etl_name,parameterkey,parametervalue) VALUES ('zetl','stock','" + self.stock + "')")
		self.cur.execute("INSERT INTO _zetl.z_control (etl_name,parameterkey,parametervalue) VALUES ('zetl','stgtablename','" + self.stgtablename + "')")

	def setsqlfile(self,sqlfile):
		self.sqlfile = sqlfile
		self.cur.execute("DELETE FROM _zetl.z_control WHERE etl_name='zetl' and parameterkey in ('sqlfile')")
		self.cur.execute("INSERT INTO _zetl.z_control (etl_name,parameterkey,parametervalue) VALUES ('zetl','sqlfile','" + self.sqlfile + "')")

	def load_etl_job_details(self,etl_name_or_num):
		foundit = 0
		ReturnThis = ""

		zsql = """SELECT DISTINCT etl_name,stepnum,coalesce(steptablename,''),sql_to_run,sqlfile,table_or_view,note,dtm
				  FROM _zetl.z_etl WHERE lower(etl_name) = '""" + etl_name_or_num + "' or lower(cast(stepnum as varchar)) = '" + etl_name_or_num + "'  ORDER BY stepnum"

		#print(zsql)
		RowsReturned = self.cur.execute(zsql)
		try:
			data=self.cur.fetchall()

			for row in data:
				self.loaded = 1
				newetl_step = etl_step()
				newetl_step.etl_name		= row[0]
				newetl_step.stepnum			= row[1]
				newetl_step.steptablename	= row[2]
				newetl_step.sql				= row[3]
				newetl_step.sqlfile			= variables().dir_zetl_scripts + '\\' + row[0] + '\\' + row[4]
				newetl_step.table_or_view	= row[5]
				newetl_step.note			= row[6]
				newetl_step.dtm				= row[7]
				foundit = 1

				#print(newetl_step.sqlfile)
				#exit(0)

				self.steps.append(newetl_step)

			if foundit == 0:
				print('\nCannot find '+ etl_name_or_num + ' in z_etl \n')

		except Exception as e:
			print(str(e))
			pass


