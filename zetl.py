"""
  Dave Skura, Dec,2022
"""
from zetldbfile import zetldbaccess

import warnings
import sys
import os
import re
from datetime import *

class zetl:
	def __init__(self):
		#now = (datetime.now())
		#sztoday=str(now.year) + '-' + ('0' + str(now.month))[-2:] + '-' + str(now.day)

		self.force = True

		self.zetldb = zetldbaccess()

	def logstepstart(self,etl_name,stepnum,cmdfile,steptablename,query,ipart):

		zsql = "INSERT INTO " + self.DB_SCHEMA() + ".z_log (etl_name,dbuser,stepnum,cmdfile,steptablename,"
		zsql += "cmd_to_run,part,rundtm) VALUES ('" + etl_name + "',(SELECT current_user),"
		zsql += str(stepnum) + ",'" + str(cmdfile) + "','" + steptablename + "','" 
		zsql += query.replace('?','').replace("'","`") + "'," + str(ipart) + ", now()::timestamp);"
		self.execute(zsql)
		
		lid = self.queryone("SELECT max(id) FROM " + self.DB_SCHEMA() + ".z_log ")
		return lid

	def logstepend(self,lid,the_rowcount,consoleoutput='not-passed-in',database='not-passed-in'):
		usql = "UPDATE  " + self.DB_SCHEMA() + ".z_log SET "

		if consoleoutput!='not-passed-in':
			usql += "script_output='" + consoleoutput.replace("'",'`') + "',"

		if database!='not-passed-in':
			usql += "database='" + database.replace("'",'`') + "',"

		usql += "rowcount = " + str(the_rowcount) + ", endtime = now()::timestamp WHERE id = " + str(lid) 

		try:
			self.execute(usql)
		except Exception as e:
			print(str(e))
			sys.exit(1) 


	def f1(self,foo=''): return iter(foo.splitlines())

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

	def log_script_error(self,lid,script_error,database='',script_output=''):

		usql = "UPDATE  " + self.DB_SCHEMA() + ".z_log SET database='" + database.replace("'",'`') + "', script_output = '" + script_output.replace("'","`") + "', script_error = '" + script_error.replace("'","`") + "', endtime = now()::timestamp WHERE id = " + str(lid) 
		try:
			self.zetldb.db.execute(usql)
			self.zetldb.db.commit()
		except Exception as e:
			print(str(e))

	def run_one_etl_step(self,etl_name,stepnum,steptablename,cmdfile):

		script_variables = {'DB_TYPE':'','DB_USERNAME':'','DB_USERPWD':'','DB_HOST':'','DB_PORT':'','DB_NAME':'','DB_SCHEMA':''}

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

		sql = self.RemoveComments(sqlfromfile.strip())

		ipart = 0
		for individual_query in sql.split(';'):

			ipart += 1
			individual_query = individual_query.strip()
			if not individual_query.isspace() and individual_query != '':
				script_output = ''

				print('\n file ' + cmdfile + ', step ' + str(ipart))
				print(individual_query)
				#script_output += individual_query + '\n \n'

				lid = self.logstepstart(etl_name,stepnum,cmdfile,steptablename,individual_query,ipart)
				database = ''

				try:
					if script_variables['DB_TYPE'] != '': # dont use default connection
						new_postgresdb = db()
						new_mysqldb = mysql_db()

						if script_variables['DB_TYPE'].strip().upper() == 'POSTGRES':
							script_output, database = self.connect_and_run(script_variables,new_postgresdb,individual_query)
							database = script_variables['DB_TYPE'] + ': ' + database
							self.logend_steptable(new_postgresdb,lid,script_variables,steptablename,script_output,database)

						elif script_variables['DB_TYPE'].strip().upper() == 'MYSQL':
							script_output, database = self.connect_and_run(script_variables,new_mysqldb,individual_query)
							database = script_variables['DB_TYPE'] + ': ' + database
							self.logend_steptable(new_mysqldb,lid,script_variables,steptablename,script_output,database)

						else:
							print('DB_TYPE must be either Postgres or MySQL')
							sys.exit(0)


					else: # use default connection
						database = 'default: ' + self.zetldb.db.dbstr()
						if individual_query.strip().upper().find('SELECT') == 0:
							results = self.zetldb.db.export_query_to_str(individual_query)
							print('\n' + results)
							script_output += results

						else:

							self.zetldb.db.execute(individual_query)

						self.logend_steptable(self.zetldb.db,lid,script_variables,steptablename,script_output,database)
				except Exception as e:
					self.log_script_error(lid,str(e),database, script_output)
					print(str(e))
					sys.exit(1)

	def connect_and_run(self,script_variables,dbconn,individual_query):
		script_output = ''

		dbconn.useConnectionDetails(script_variables['DB_USERNAME']
						,script_variables['DB_USERPWD']
						,script_variables['DB_HOST']
						,script_variables['DB_PORT']
						,script_variables['DB_NAME']
						,script_variables['DB_SCHEMA'])

		if individual_query.strip().upper().find('SELECT') == 0:
			results = dbconn.export_query_to_str(individual_query)
			script_output += results
			print('\n' + results)

		else:

			dbconn.execute(individual_query)
			dbconn.commit()

		return script_output, dbconn.dbstr()


	def logend_steptable(self,dbconn,lid,script_variables,steptablename,script_output,database=''):
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
				this_schema = dbconn.db_conn_dets.DB_SCHEMA
				this_table = steptablename.split('.')[0]

		qualified_table = this_schema + '.' + this_table
		tblrowcount = 0
		if dbconn.does_table_exist(qualified_table):
			tblrowcount = dbconn.queryone("SELECT COUNT(*) FROM " + qualified_table)
			dbconn.close()

		self.logstepend(lid,tblrowcount,script_output,database)


	def get_current_activity(self):
		sql = """
			SELECT *
			FROM (
					SELECT currently,activity_type FROM """ + self.DB_SCHEMA() + """.z_activity
					UNION
					SELECT '' as currently,'default' as activity_type
					) L
			ORDER BY 1 desc
		"""
		data = self.zetldb.db.query(sql)

		if data[0][1] == 'default':
			return_value = 'idle'
		else:
			return_value = data[0][0] 

		return return_value

	def runetl(self,etl_name):
		tempfilename = 'zetl_pythonscript_temp.log'
		try:
			os.remove(tempfilename)
		except:
			pass
		sql = """
		SELECT stepnum,steptablename,cmdfile 
		FROM """ + self.DB_SCHEMA() + """.z_etl 
		WHERE etl_name = '""" + etl_name + """'
		ORDER BY etl_name, stepnum
		"""
		#print(sql)
		data = self.zetldb.db.query(sql)
		for row in data:
			stepnum = row[0]
			steptablename = row[1]
			cmdfile = row[2]
			foundfile = '.\\zetl_scripts\\' + etl_name + '\\' + cmdfile
			#print('stepnum = \t\t' + str(stepnum))
			#print('steptablename = \t' + steptablename)
			#print('cmdfile = \t\t' + cmdfile)
			if cmdfile.lower().endswith('.sql') or cmdfile.lower().endswith('.ddl'):
				self.run_one_etl_step(etl_name,stepnum,steptablename,cmdfile)

			elif cmdfile.lower().endswith('.py'):

				lid = self.logstepstart(etl_name,stepnum,cmdfile,steptablename,'Python script',0)

				print('\n file ' + cmdfile + '\n')

				exit_code = os.system('py ' + foundfile  + ' > ' + tempfilename)

				f = open(tempfilename,'r')
				consoleoutput = f.read()
				f.close()

				if exit_code != 0:
					print(consoleoutput + '\n exit_code=' + str(exit_code))
					self.log_script_error(lid,foundfile + ' failed with error_code ' + str(exit_code),self.zetldb.db.dbstr(),consoleoutput.strip())
					self.logstepend(lid,0)

					sys.exit(exit_code)

				print(consoleoutput)
				self.logstepend(lid,0,consoleoutput.strip(),self.zetldb.db.dbstr())

			elif cmdfile.lower().endswith('.bat'):
				lid = self.logstepstart(etl_name,stepnum,cmdfile,steptablename,'Windows bat script',0)

				print('\n file ' + cmdfile + '\n')
				exit_code = os.system(foundfile + ' > ' + tempfilename)
				f = open(tempfilename,'r')
				consoleoutput = f.read()
				f.close()
				
				if exit_code != 0:
					print(consoleoutput + '\n exit_code=' + str(exit_code))
					self.log_script_error(lid,foundfile + ' failed with error_code ' + str(exit_code),self.zetldb.db.dbstr(),consoleoutput.strip())
					self.logstepend(lid,0)

					sys.exit(exit_code)

				print(consoleoutput)
				self.logstepend(lid,0,consoleoutput.strip(),self.zetldb.db.dbstr())

	def show_etl_name_list(self):
		data = self.zetldb.db.query('SELECT distinct etl_name from ' + self.DB_SCHEMA() + '.z_etl order by etl_name')
		for row in data:
			print(' ' + row[0])

	def DB_SCHEMA(self):
		return self.zetldb.db.db_conn_dets.DB_SCHEMA

	def execute(self,prm):
		return self.zetldb.db.execute(prm)

	def commit(self):
		return self.zetldb.db.commit()

	def queryone(self,prm):
		return self.zetldb.db.queryone(prm)

if __name__ == '__main__':
	my_zetl = zetl()
	if len(sys.argv) == 1 or sys.argv[1] == 'zetl.py': # no parameters
		print('usage: ')
		print('zetl.py [etl_name] ') 
		print('')
		my_zetl.zetldb.empty_zetl()						# empty the master zetl table
		my_zetl.zetldb.load_folders_to_zetl() # load master zetl table from folders
		#my_zetl.zetldb.export_zetl() # exporting from the database to csv file.
		my_zetl.show_etl_name_list()

	else: # run the etl match the etl_name in the etl table
		etl_name_to_run = sys.argv[1]
		run_options=''
		run_description = ''
		if len(sys.argv) == 3:
			run_options = sys.argv[2]
			if run_options == '-f':
				run_description = '  But first, overwriting table ' + my_zetl.DB_SCHEMA() + '.z_etl with file zetl_scripts\\' + etl_name_to_run + '\\z_etl.csv'

		print('Running ' + etl_name_to_run + '.' + run_description)

		my_zetl.zetldb.load_folders_to_zetl(etl_name_to_run)
		if run_options == '-f':
			my_zetl.zetldb.load_z_etlcsv_if_forced(etl_name_to_run,run_options)
		else:
			my_zetl.zetldb.export_zetl()

		activity = my_zetl.get_current_activity()
		if activity == 'idle' or my_zetl.force:
			my_zetl.execute('DELETE FROM ' + my_zetl.DB_SCHEMA() + '.z_activity')
			my_zetl.execute("INSERT INTO " + my_zetl.DB_SCHEMA() + ".z_activity(currently,previously) VALUES ('Running " + etl_name_to_run + "','" + activity + "')")

			my_zetl.runetl(etl_name_to_run)

			my_zetl.execute("UPDATE " + my_zetl.DB_SCHEMA() + ".z_activity SET currently = 'idle',previously='Running " + etl_name_to_run + "'")
			my_zetl.commit()

		else:
			print("zetl is currently busy with '" + activity + "'")


	sys.exit(0)


		

