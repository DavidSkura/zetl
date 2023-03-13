"""
  Dave Skura, Dec,2022
"""
from postgresdave_package.postgresdave import postgres_db #install pip install postgresdave-package
from mysqldave_package.mysqldave import mysql_db #install pip install mysqldave-package

import warnings
import sys
import os
import re
from datetime import *

def main():
	my_zetl = zetl()
	if len(sys.argv) == 1 or sys.argv[1] == 'run.py': # no parameters
		print('')
		print('usage: ')
		print('py -m zetl.run [etl_name] ') 
		print('py -m zetl.view [etl_name] ') 
		print(' ')
		print('py -m zetl.postgres_import [csv_filename] [tablename] [WithTruncate]') 
		print('py -m zetl.postgres_export [tablename] [csvfilename] [delimiter] ') 
		print(' ')
		print('py -m zetl.mysql_import [csv_filename] [tablename] [WithTruncate]') 
		print('py -m zetl.mysql_export [tablename] [csvfilename] [delimiter] ') 
		print('-----------')
		my_zetl.zetldb.empty_zetl()						# empty the master zetl table
		my_zetl.zetldb.load_folders_to_zetl() # load master zetl table from folders
		my_zetl.show_etl_name_list()

	else: # run the etl match the etl_name in the etl table
		etl_name_to_run = sys.argv[1]
		run_parameter = ''
		if len(sys.argv) > 2:
			run_parameter = sys.argv[2] # ARGV1 in the .sql files in zetl_scripts
		my_zetl.proper_run(etl_name_to_run,run_parameter)

	sys.exit(0)


class zetl:
	def forcerun(self,etl_name_to_run,run_parameter=''):
		self.force = True
		self.proper_run(etl_name_to_run,run_parameter)

	def proper_run(self,etl_name_to_run,run_parameter=''):
		print('Running ' + etl_name_to_run)
		self.zetldb.load_folders_to_zetl(etl_name_to_run)
		self.zetldb.export_zetl()

		activity = self.get_current_activity()
		if activity == 'idle' or self.force:
			self.execute('DELETE FROM ' + self.DB_SCHEMA() + '.z_activity')
			self.execute("INSERT INTO " + self.DB_SCHEMA() + ".z_activity(currently,previously) VALUES ('Running " + etl_name_to_run + "','" + activity + "')")

			print(etl_name_to_run)
			self.runetl(etl_name_to_run,run_parameter)

			self.execute("UPDATE " + self.DB_SCHEMA() + ".z_activity SET currently = 'idle',previously='Running " + etl_name_to_run + "'")
			self.commit()

		else:
			print("zetl is currently busy with '" + activity + "'.  You can wait for it to finish or call zetl.forcerun(etlname,run_parameter).")

	def __init__(self):
		#now = (datetime.now())
		#sztoday=str(now.year) + '-' + ('0' + str(now.month))[-2:] + '-' + str(now.day)
		self.silent_on = False
		self.tempfilename = 'zetl_pythonscript_temp.log'

		self.force = True

		self.zetldb = zetldbaccess()

	def logstepstart(self,etl_name,stepnum,cmdfile,steptablename,query,ipart):

		try:
			zsql = "INSERT INTO " + self.DB_SCHEMA() + ".z_log (etl_name,dbuser,stepnum,cmdfile,steptablename,"
			zsql += "cmd_to_run,part,rundtm) VALUES ('" + etl_name + "',(SELECT current_user),"
			zsql += str(stepnum) + ",'" + str(cmdfile) + "','" + steptablename + "','" 
			zsql += query.replace('?','').replace("'","`") + "'," + str(ipart) + ", now()::timestamp);"
			self.execute(zsql)
		except:
			self.zetldb.create_table('z_log')
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

	def log_script_error(self,lid,pscript_error,database='',pscript_output=''):
		script_error = pscript_error
		if len(pscript_error) > 250:
			script_error = pscript_error[250:]

		script_output = pscript_output
		if len(pscript_output) > 8100:
			script_output = pscript_output[-8100:]


		usql = "UPDATE  " + self.DB_SCHEMA() + ".z_log SET database='" + database.replace("'",'`') + "', script_output = '" + script_output.replace("'","`") + "', script_error = '" + script_error.replace("'","`") + "', endtime = now()::timestamp WHERE id = " + str(lid) 
		try:
			self.execute(usql)
			self.commit()
		except Exception as e:
			print(str(e))

	def run_one_etl_step(self,etl_name,stepnum,steptablename,cmdfile,run_parameter=''):

		script_variables = {'DB_TYPE':'','DB_USERNAME':'','DB_USERPWD':'','DB_HOST':'','DB_PORT':'','DB_NAME':'','DB_SCHEMA':'','ARGV1':run_parameter}

		findcmdfile = '.\\zetl_scripts\\' + etl_name + '\\' + cmdfile
		try:
			f = open(findcmdfile,'r') 
			sqlfromfile = f.read()
			sqlfromfile = sqlfromfile.replace('<ARGV1>',script_variables['ARGV1'])

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
						new_postgresdb = postgres_db()
						new_mysqldb = mysql_db()

						if script_variables['DB_TYPE'].strip().upper() == 'POSTGRES':
							self.connect_mysql(new_postgresdb,script_variables)

							script_output, database = self.run_db_query(new_postgresdb,individual_query)
							database = script_variables['DB_TYPE'] + ': ' + database
							self.logend_steptable(new_postgresdb,lid,script_variables,steptablename,script_output,database)

						elif script_variables['DB_TYPE'].strip().upper() == 'MYSQL':
							self.connect_mysql(new_mysqldb,script_variables)

							script_output, database = self.run_db_query(new_mysqldb,individual_query)
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

	def connect_postgres(self,postgresdb,script_variables):
		if script_variables['DB_HOST'] != '': # variables set in file.  use them to connect
			postgresdb.useConnectionDetails(script_variables['DB_USERNAME']
							,script_variables['DB_USERPWD']
							,script_variables['DB_HOST']
							,script_variables['DB_PORT']
							,script_variables['DB_NAME']
							,script_variables['DB_SCHEMA']
							)
		else: # use default mysql connection
			postgresdb.connect()
		print(postgresdb.dbstr())

	def connect_mysql(self,mysqldb,script_variables):
		if script_variables['DB_HOST'] != '': # variables set in file.  use them to connect
			mysqldb.useConnectionDetails(script_variables['DB_USERNAME']
							,script_variables['DB_USERPWD']
							,script_variables['DB_HOST']
							,script_variables['DB_PORT']
							,script_variables['DB_NAME']
							)
		else: # use default mysql connection
			mysqldb.connect()
		print(mysqldb.dbstr())


	def run_db_query(self,dbconn,individual_query):
		script_output = ''

		if individual_query.strip().upper().find('SELECT') == 0:
			results = dbconn.export_query_to_str(individual_query)
			script_output += results
			print('\n' + results)

		else:

			dbconn.execute(individual_query)
			dbconn.commit()

		return script_output, dbconn.dbstr()


	def logend_steptable(self,dbconn,lid,script_variables,steptablename,script_output,database=''):
		tblrowcount = 0
		qualified_table = ''
		if script_variables['DB_TYPE'] != '': # dont use default connection
			if script_variables['DB_TYPE'].upper() == 'MYSQL':
				qualified_table = steptablename
			elif (script_variables['DB_TYPE'].upper() == 'POSTGRES'):
				try:
					this_table = steptablename.split('.')[1]
					this_schema = steptablename.split('.')[0]
				except:
					this_table = steptablename
					if script_variables['DB_SCHEMA'] != '':
						this_schema = script_variables['DB_SCHEMA']
					else:
						this_schema = 'public'
				qualified_table = this_schema + '.' + this_table
				if dbconn.does_table_exist(qualified_table):
					tblrowcount = dbconn.queryone("SELECT COUNT(*) FROM " + qualified_table)
					dbconn.close()

			else:
				print('DB_TYPE must be Postgres or MySQL')

		else: # use default postgres connection
			try:
				this_table = steptablename.split('.')[1]
				this_schema = steptablename.split('.')[0]
			except:
				this_table = steptablename
				if script_variables['DB_SCHEMA'] != '':
					this_schema = script_variables['DB_SCHEMA']
				else:
					this_schema = 'public'
			qualified_table = this_schema + '.' + this_table
			if dbconn.does_table_exist(qualified_table):
				tblrowcount = dbconn.queryone("SELECT COUNT(*) FROM " + qualified_table)
				dbconn.close()

		self.logstepend(lid,tblrowcount,script_output,database)


	def get_current_activity(self):
		try:
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

		except:

			self.zetldb.create_table('z_activity')
			data = self.zetldb.db.query(sql)

		if data[0][1] == 'default':
			return_value = 'idle'
		else:
			return_value = data[0][0] 

		return return_value

	def runetl(self,etl_name,run_parameter=''):
		if self.silent_on:
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
			consoleoutput = ''
			stepnum = row[0]
			steptablename = row[1]
			cmdfile = row[2]
			foundfile = '.\\zetl_scripts\\' + etl_name + '\\' + cmdfile
			#print('stepnum = \t\t' + str(stepnum))
			#print('steptablename = \t' + steptablename)
			#print('cmdfile = \t\t' + cmdfile)
			if cmdfile.lower().endswith('.sql') or cmdfile.lower().endswith('.ddl'):
				self.run_one_etl_step(etl_name,stepnum,steptablename,cmdfile,run_parameter)

			elif cmdfile.lower().endswith('.py'):

				lid = self.logstepstart(etl_name,stepnum,cmdfile,steptablename,'Python script',0)

				print('\n file ' + cmdfile + '\n')
				cmd_to_run = 'py ' + foundfile + ' ' + run_parameter
				if self.silent_on:
					cmd_to_run += ' > ' + tempfilename

				print(cmd_to_run)
				exit_code = os.system(cmd_to_run)

				if self.silent_on:
					f = open(tempfilename,'r')
					consoleoutput = f.read()
					f.close()

				if exit_code != 0:
					print(consoleoutput + '\n exit_code=' + str(exit_code))
					self.log_script_error(lid,foundfile + ' failed with error_code ' + str(exit_code),self.zetldb.db.dbstr(),consoleoutput.strip())
					self.logstepend(lid,0)

					sys.exit(exit_code)

				self.logstepend(lid,0,consoleoutput.strip(),self.zetldb.db.dbstr())

			elif cmdfile.lower().endswith('.bat'):
				lid = self.logstepstart(etl_name,stepnum,cmdfile,steptablename,'Windows bat script',0)

				print('\n file ' + cmdfile + '\n')
				cmd_to_run = foundfile + ' ' + run_parameter
				if self.silent_on:
					cmd_to_run += ' > ' + tempfilename

				exit_code = os.system(cmd_to_run)

				if self.silent_on:
					f = open(tempfilename,'r')
					consoleoutput = f.read()
					f.close()
				
				if exit_code != 0:
					print(consoleoutput + '\n exit_code=' + str(exit_code))
					self.log_script_error(lid,foundfile + ' failed with error_code ' + str(exit_code),self.zetldb.db.dbstr(),consoleoutput.strip())
					self.logstepend(lid,0)

					sys.exit(exit_code)

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


class zetldbaccess:

	def __init__(self):
		self.db = postgres_db()
		self.db.connect()

		self.version=2.0

	def	createtables(self):
		install_ddl = '.\\install_ddl\\'
		ddl_files = os.listdir(install_ddl)
		for i in range(0,len(ddl_files)):
			ddl_file = ddl_files[i]
			tablename = ddl_file.split('.')[0]
			
			print('Found ' + ddl_file)
			if not self.db.does_table_exist(tablename):

				print('Table ' + tablename + ' does not exist.  Creating it from file: '  + ddl_file)

				fh = open(install_ddl + ddl_file,'r')
				ddl = fh.read()
				fh.close()

				# add schema prefix
				ddl = ddl.replace('CREATE TABLE ','CREATE TABLE ' + self.db.db_conn_dets.DB_SCHEMA + '.')
				ddl = ddl.replace('COMMENT ON TABLE ','COMMENT ON TABLE ' + self.db.db_conn_dets.DB_SCHEMA + '.')
				try:
					self.db.execute(ddl)
					print('Table ' + tablename + ' created. ')
				except Exception as e:
					raise Exception('Cannot create table ' + tablename + '\n' + str(e))
					
	def export_zetl(self,etl_name =''):
		zsql = ' SELECT DISTINCT etl_name FROM ' + self.db.db_conn_dets.DB_SCHEMA + '.z_etl '
		if etl_name != '':
			zsql += "WHERE upper(etl_name) like upper('" + etl_name + "') "
		zsql += ' ORDER BY etl_name '
		etl_list = self.db.query(zsql)
		for etl in etl_list:
			etl_name = etl[0]
			qry = "SELECT stepnum,cmdfile,steptablename,estrowcount FROM " + self.db.db_conn_dets.DB_SCHEMA + ".z_etl WHERE etl_name = '" + etl_name + "' ORDER BY stepnum"
			csv_filename = 'zetl_scripts\\' + etl_name + '\\z_etl.csv'
			self.db.export_query_to_csv(qry,csv_filename)

	def empty_zetl(self,etl_name=''):
		try:
			dsql = "DELETE FROM " + self.db.db_conn_dets.DB_SCHEMA + ".z_etl "
			if etl_name != '':
				dsql += " WHERE upper(etl_name) like upper('" + etl_name + "') "
			self.db.execute(dsql)
			self.db.commit()
		except:
			self.create_table('z_etl')

	def create_table(self,tbl):
		csql = "SELECT 'cannot create table " + tbl + "' as CREATE "
		if tbl.lower() == 'z_etl':
			csql = """
			CREATE TABLE """ + self.db.db_conn_dets.DB_SCHEMA + """.z_etl (
				etl_name varchar(100) DEFAULT NULL, 
				stepnum NUMERIC(10,2) DEFAULT NULL, 
				steptablename varchar(250) DEFAULT '', 
				estrowcount BIGINT  DEFAULT -1, 
				cmdfile varchar(250) DEFAULT '', 
				cmd_to_run varchar(12000) DEFAULT '', 
				note varchar(1024) DEFAULT '', 
				dtm TIMESTAMP DEFAULT now()::timestamp 
			);

			COMMENT ON TABLE """ + self.db.db_conn_dets.DB_SCHEMA + """.z_etl IS 'This is the Master etl table.  When calling run_etl.py <etl_name> .. all lines matching etl_name here are executed.';
			"""
		elif tbl.lower() == 'z_activity':
			csql = """
			CREATE TABLE """ + self.db.db_conn_dets.DB_SCHEMA + """.z_activity (
				activity_type varchar(250) Primary key DEFAULT 'Primary', 
				currently varchar(250) DEFAULT 'idle', 
				previously varchar(250) DEFAULT NULL, 
				keyfld varchar(250) DEFAULT '', 
				prvkeyfld varchar(250) DEFAULT '', 
				dtm TIMESTAMP DEFAULT now()::timestamp
			);

			"""
		elif tbl.lower() == 'z_log':
			csql = """
			CREATE TABLE """ + self.db.db_conn_dets.DB_SCHEMA + """.z_log (
				id SERIAL NOT NULL primary key, 
				dbuser varchar(100) DEFAULT NULL, 
				rundtm timestamp DEFAULT now()::timestamp, 
				etl_name varchar(100) DEFAULT NULL, 
				stepnum NUMERIC(5,1) DEFAULT NULL, 
				part INTEGER DEFAULT 1, 
				steptablename varchar(250) DEFAULT NULL, 
				rowcount BIGINT, 
				starttime TIMESTAMP DEFAULT now()::timestamp,  
				endtime TIMESTAMP, 
				cmd_to_run varchar(8124) DEFAULT NULL, 
				script_output varchar(8124) DEFAULT NULL, 
				script_error varchar(255) DEFAULT NULL, 
				cmdfile varchar(250) DEFAULT NULL, 
				database varchar(250) DEFAULT NULL, 
				dtm TIMESTAMP DEFAULT now()::timestamp
			);

			COMMENT ON TABLE """ + self.db.db_conn_dets.DB_SCHEMA + """.z_log IS 'Global log table.  Every sql which is run from zetl.py is logged here.';
			"""
		self.db.execute(csql)
		self.db.commit()

	def load_z_etlcsv_if_forced(self,etl_name='',option=''):
		szdelimiter = ','
		if (etl_name != '' and option == '-f'):
			self.empty_zetl()
			qualified_table = self.db.db_conn_dets.DB_SCHEMA + ".z_etl"
			csv_filename = 'zetl_scripts\\' + etl_name + '\\z_etl.csv'
			f = open(csv_filename,'r')
			hdrs = f.read(1000).split('\n')[0].strip().split(szdelimiter)
			f.close()		
			
			isqlhdr = 'INSERT INTO ' + qualified_table + '('

			for i in range(0,len(hdrs)):
				isqlhdr += hdrs[i] + ','
			isqlhdr = isqlhdr[:-1] + ') VALUES '

			skiprow1 = 0
			ilines = ''

			with open(csv_filename) as myfile:
				for line in myfile:
					if skiprow1 == 0:
						skiprow1 = 1
					else:
						row = line.rstrip("\n").split(szdelimiter)

						newline = '('
						for j in range(0,len(row)):
							if row[j].lower() == 'none' or row[j].lower() == 'null':
								newline += "NULL,"
							else:
								newline += "'" + row[j].replace(',','').replace("'",'') + "',"
							
						ilines += newline[:-1] + ')'
						
						qry = isqlhdr + ilines
						ilines = ''
						self.db.execute(qry)
						self.db.commit()

	def is_an_int(self,prm):
			try:
				if int(prm) == int(prm):
					return True
				else:
					return False
			except:
					return False

	def add_etl_step(self,p_etl_name,p_etl_step,p_etl_filename):
		isql = "INSERT INTO " + self.db.db_conn_dets.DB_SCHEMA + ".z_etl(etl_name,stepnum,cmdfile) VALUES ('" + p_etl_name + "'," + p_etl_step + ",'" + p_etl_filename + "')"
		self.db.execute(isql)
		self.db.commit()
		#print('Adding ' + p_etl_name + '\\' + p_etl_filename)

	def etl_step_exists(self,etl_name,etl_step):
		sql = "SELECT COUNT(*) FROM " + self.db.db_conn_dets.DB_SCHEMA + ".z_etl WHERE etl_name = '" + etl_name + "' and stepnum = " + etl_step
		etlrowcount = self.db.queryone(sql)
		if etlrowcount == 0:
			return False
		else:
			return True

	def load_folders_to_zetl(self,this_etl_name='all'):
		etl_folder = 'zetl_scripts'
		subdirs = [x[0] for x in os.walk(etl_folder)]
		for i in range(0,len(subdirs)):
			possible_etl_dir = subdirs[i]
			if len(possible_etl_dir.split('\\')) == 2:
				etl_name = possible_etl_dir.split('\\')[1]
				if (this_etl_name == 'all' or etl_name == this_etl_name):
					
					dir_list = os.listdir(etl_folder + '\\' + etl_name)
					for etl_script_file in os.listdir(etl_folder + '\\' + etl_name):
						if etl_script_file.endswith(".sql") or etl_script_file.endswith(".ddl") or etl_script_file.endswith(".py") or etl_script_file.endswith(".bat"):
							if len(etl_script_file.split('.')) == 3:
								etl_step = etl_script_file.split('.')[0]
								file_suffix = etl_script_file.split('.')[1] + '.' + etl_script_file.split('.')[2]
								if self.is_an_int(etl_step):
									if not self.etl_step_exists(etl_name,etl_step):
										self.add_etl_step(etl_name,etl_step,etl_script_file)		

if __name__ == '__main__':
	main()

	#zetl().proper_run('test','')

		


