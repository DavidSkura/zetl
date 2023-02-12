"""
  Dave Skura, 2022
"""
import os
import sys
import psycopg2 

class dbinfo: 
	def __init__(self): 
		self.version=1.0 
		self.DatabaseType='Postgres' 
		self.updated='Dec 2/2022' 
 
		self.DB_USERNAME='postgres' 
		self.DB_USERPWD='4165605869' 
		self.DB_HOST='localhost' 
		self.DB_PORT='1532' 
		self.DB_NAME='postgres' 
		self.DB_SCHEMA='weather' 
		self.dbconnectionstr= self.DatabaseType + '/' + self.DB_USERNAME + '@' + self.DB_HOST + ':' + self.DB_HOST + '/' + self.DB_NAME + '/' + self.DB_SCHEMA 

class db:
	def get_dbversion(self):
		return self.queryone('SELECT VERSION()')

	def __init__(self):
		self.version=1.0
		
		# ***** edit these DB credentials for the installation to work *****
		self.ihost = dbinfo().DB_HOST				# 'localhost'
		self.iport = dbinfo().DB_PORT				#	"5432"
		self.idb = dbinfo().DB_NAME					# 'nfl'
		self.ischema = dbinfo().DB_SCHEMA		#	'_raw'
		if self.ischema == '':
			self.ischema = 'public'
		self.iuser = dbinfo().DB_USERNAME		#	'dad'
		self.ipwd = dbinfo().DB_USERPWD			#	'dad'
		self.connection_str = dbinfo().dbconnectionstr

		self.dbconn = None
		self.cur = None

	def export_query_to_csv(self,qry,csv_filename,szdelimiter=','):
		self.cur.execute(qry)
		f = open(csv_filename,'w')
		sz = ''
		for k in [i[0] for i in self.cur.description]:
			sz += k + szdelimiter
		f.write(sz[:-1] + '\n')

		for row in self.cur:
			sz = ''
			for i in range(0,len(self.cur.description)):
				sz += str(row[i])+ szdelimiter

			f.write(sz[:-1] + '\n')
				

	def export_table_to_csv(self,csvfile,tblname,szdelimiter=','):
		if not self.does_table_exist(tblname):
			raise Exception('Table does not exist.  Create table first')

		this_schema = tblname.split('.')[0]
		try:
			this_table = tblname.split('.')[1]
		except:
			this_schema = self.ischema
			this_table = tblname.split('.')[0]

		qualified_table = this_schema + '.' + this_table

		self.export_query_to_csv('SELECT * FROM ' + qualified_table,csvfile,szdelimiter)

	def load_csv_to_table(self,csvfile,tblname,withtruncate=True,szdelimiter=',',fields='',withextrafields={}):
		this_schema = tblname.split('.')[0]
		try:
			this_table = tblname.split('.')[1]
		except:
			this_schema = self.ischema
			this_table = tblname.split('.')[0]

		qualified_table = this_schema + '.' + this_table

		if not self.does_table_exist(tblname):
			raise Exception('Table does not exist.  Create table first')

		if withtruncate:
			self.execute('TRUNCATE TABLE ' + qualified_table)

		f = open(csvfile,'r')
		hdrs = f.read(1000).split('\n')[0].strip().split(szdelimiter)
		f.close()		

		isqlhdr = 'INSERT INTO ' + qualified_table + '('

		if fields != '':
			isqlhdr += fields	+ ') VALUES '	
		else:
			for i in range(0,len(hdrs)):
				isqlhdr += hdrs[i] + ','
			isqlhdr = isqlhdr[:-1] + ') VALUES '

		skiprow1 = 0
		batchcount = 0
		ilines = ''

		with open(csvfile) as myfile:
			for line in myfile:
				if line.strip()!='':
					if skiprow1 == 0:
						skiprow1 = 1
					else:
						batchcount += 1
						row = line.rstrip("\n").split(szdelimiter)
						newline = "("
						if 'loadfile' in withextrafields: # loadfile, stationid, province
							newline += "'" + csvfile + "',"
						if 'stationid' in withextrafields: # loadfile, stationid,province
							newline += "'" + csvfile.split('/')[2].split('_')[0] + "',"
						if 'province' in withextrafields: # loadfile, stationid, province
							newline += "'" + withextrafields['province'] + "',"

						for j in range(0,len(row)):
							if row[j].lower() == 'none' or row[j].lower() == 'null':
								newline += "NULL,"
							else:
								newline += "'" + row[j].replace(',','').replace("'",'').replace('"','') + "',"
							
						ilines += newline[:-1] + '),'
						
						if batchcount > 500:
							qry = isqlhdr + ilines[:-1]
							batchcount = 0
							ilines = ''
							self.execute(qry)

		if batchcount > 0:
			qry = isqlhdr + ilines[:-1]
			batchcount = 0
			ilines = ''
			self.execute(qry)


	def does_table_exist(self,tblname):
		# tblname may have a schema prefix like public.sales
		#		or not... like sales

		try:
			this_schema = tblname.split('.')[0]
			this_table = tblname.split('.')[1]
		except:
			this_schema = self.ischema
			this_table = tblname.split('.')[0]

		sql = """
			SELECT count(*)  
			FROM information_schema.tables
			WHERE table_schema = '""" + this_schema + """' and table_name='""" + this_table + "'"
		
		if self.queryone(sql) == 0:
			return False
		else:
			return True

	def close(self):
		if self.dbconn:
			self.dbconn.close()

	def connect(self):
		p_options = "-c search_path=" + self.ischema
		try:
			self.dbconn = psycopg2.connect(
					host=self.ihost,
					database=self.idb,
					user=self.iuser,
					password=self.ipwd,
					options=p_options
					#autocommit=True
			)
			self.dbconn.set_session(autocommit=True)
			self.cur = self.dbconn.cursor()
		except Exception as e:
			raise Exception(str(e))

	def query(self,qry):
		if not self.dbconn:
			self.connect()

		self.cur.execute(qry)
		all_rows_of_data = self.cur.fetchall()
		return all_rows_of_data

	def commit(self):
		self.dbconn.commit()

	def close(self):
		self.dbconn.close()

	def execute(self,qry):
		try:
			if not self.dbconn:
				self.connect()
			self.cur.execute(qry)
		except Exception as e:
			raise Exception("SQL ERROR:\n\n" + str(e))

	def queryone(self,select_one_fld):
		try:
			if not self.dbconn:
				self.connect()
			self.execute(select_one_fld)
			retval=self.cur.fetchone()
			return retval[0]
		except Exception as e:
			raise Exception("SQL ERROR:\n\n" + str(e))



