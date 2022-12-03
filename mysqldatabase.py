"""
  Dave Skura, 2022
"""
import mysql.connector as mysql
import socket

class db:
	def __init__(self):
		self.version=1.0
		self.dir_install_ddl = 'D:\\zetl'
		self.updated='Aug 29/22'
		self.database_hostname = 'localhost'
		self.database_ip_address = socket.gethostbyname(self.database_hostname)

		# ***** edit these DB credentials for the installation to work *****
		self.idb='MySQL'
		self.ihost= self.database_hostname # self.database_ip_address # '192.168.0.110' #'megapc' 
		self.iport="3306"
		self.ischema='nfldb'
		#self.ischema='tsx'
		self.iuser='dad'
		self.ipwd='4165605869'
		self.db='nfldb'
		self.cur=''
		self.showsql = True
		self.connection_str = self.db + '@' + self.database_hostname + ' running MySQL 8.0.29'

	def close(self):
		self.db.close()

	def connect(self):

		self.db = mysql.connect(
				host = "localhost",
				user = "dad",
				passwd = "4165605869",
				database='nfldb',
				autocommit=True
		)

		self.cur = self.db.cursor()
		#self.db.set_session(autocommit=True)

	def execute(self,qry):
		self.cur.execute(qry)

	def query(self,qry):
		self.cur.execute(qry)
		return self.cur

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
				
	def sql_to_html(self,sql):
		self.connect()
		self.cur.execute(sql)
		self.colcount = len(self.cur.description)
		self.data = self.cur.fetchall()

		hdr = """<TABLE class ="normal"><TR class ="normal">"""
		for k in [i[0] for i in self.cur.description]:
			col = '<font color=blue>' + k + '</font>'
			hdr += '<TD  class ="normal">' + col + ' </TD>'

		hdr + '</TR>'

		trows = ''
		for row in self.data :

			trows += '<TR class ="normal">'
			for i in range(0,self.colcount):
				cellvalue = str(row[i])
				trows += '<TD class ="normal">'
				if cellvalue != 'None':
					trows += cellvalue
				trows += '</TD>'

			trows += '</TR>'


		ftr = '</TABLE>'
		self.close()
		if self.showsql:
			return "<TABLE><TR><TD>" + hdr + trows + ftr + "</TD><TD valign=top align=left><pre>" + sql + "</PRE></TD></TR></TABLE>"
		else:
			return hdr + trows + ftr 

