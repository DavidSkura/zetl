"""
  Dave Skura, 2022
"""
import sys
import psycopg2 
from dbvars import dbinfo

class db:
	def __init__(self):
		self.version=1.0
		
		# ***** edit these DB credentials for the installation to work *****
		self.ihost = dbinfo().DB_HOST				# 'localhost'
		self.iport = dbinfo().DB_PORT				#	"5432"
		self.idb = dbinfo().DB_NAME					# 'nfl'
		self.ischema = dbinfo().DB_SCHEMA		#	'_raw'
		self.iuser = dbinfo().DB_USERNAME		#	'dad'
		self.ipwd = dbinfo().DB_USERPWD			#	'dad'
		self.connection_str = dbinfo().dbconnectionstr

		self.dbconn = None
		self.cur = None


	def close(self):
		if self.dbconn:
			self.dbconn.close()

	def connect(self):
		try:
			self.dbconn = psycopg2.connect(
					host=self.ihost,
					database=self.idb,
					user=self.iuser,
					password=self.ipwd
					#autocommit=True
			)
			self.dbconn.set_session(autocommit=True)
			self.cur = self.dbconn.cursor()
		except:
			raise Exception("Unable to connect to the database.")

	def execute(self,qry):
		self.cur.execute(qry)

	def queryone(self,select_one_fld):
		try:
			self.execute(select_one_fld)
			retval=self.cur.fetchone()
			return retval[0]
		except:
			raise Exception("Query failed:\n\n" + select_one_fld)

	def query_to_file(self,qry,csv_filename):
		self.cur.execute(qry)
		f = open(csv_filename,'w')
		sz = ''
		for k in [i[0] for i in self.cur.description]:
			sz += k + ','
		f.write(sz[:-1] + '\n')

		for row in self.cur:
			sz = ''
			for i in range(0,len(self.cur.description)):
				sz += str(row[i])+ ','

			f.write(sz[:-1] + '\n')
				

if len(sys.argv) == 1 or sys.argv[1] == 'zetl.py':
	try:
		print('Testing connection: ' + db().connection_str)
		zetldb = db()
		zetldb.connect()
		x = zetldb.queryone('SELECT version()')
		print(x)
		zetldb.close()
		sys.exit(0)

	except Exception as e:
		print(str(e))
		sys.exit(1)

