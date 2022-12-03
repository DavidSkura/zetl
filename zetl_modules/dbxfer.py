"""
  Dave Skura
  
  File Description:fast_import

"""
import psycopg2
import sys

from postgresdatabase import db

class fast_import:
	
	def __init__(self):
		self.version=1.0

	def import_file_to_table(self,csvfile,table):
		input_file=csvfile	# 'data\\stockupdatelist.csv'
		tablename=table			# 'stockupdatelist'
		szdelimiter=','
		mydb = db()
		thisdb = mydb.connect()

		cur = thisdb.cursor
		cur.execute('TRUNCATE TABLE ' + tablename)

		f = open(input_file,'r')
		hdrs = f.read(1000).split('\n')[0].strip().replace('<','').replace('>','').split(szdelimiter)
		f.close()		
		#sys.exit(0)

		isqlhdr = 'INSERT INTO ' + tablename + '('

		for i in range(0,len(hdrs)):
			isqlhdr += hdrs[i] + ','
		isqlhdr = isqlhdr[:-1] + ') VALUES '

		skiprow1 = 0
		batchcount = 0
		ilines = ''

		with open(input_file) as myfile:
			for line in myfile:
				if skiprow1 == 0:
					skiprow1 = 1
				else:
					batchcount += 1
					row = line.rstrip("\n").split(szdelimiter)

					newline = '('
					for j in range(0,len(row)):
						if row[j].lower() == 'none' or row[j].lower() == 'null' or row[j].lower() == '':
							newline += "NULL,"
						else:
							newline += "'" + row[j].replace(',','').replace("'",'') + "',"
						
					ilines += newline[:-1] + '),'
					
					if batchcount > 500:
						qry = isqlhdr + ilines[:-1]
						batchcount = 0
						ilines = ''
						cur.execute(qry)
						thisdb.commit()

		if batchcount > 0:
			qry = isqlhdr + ilines[:-1]
			batchcount = 0
			ilines = ''
			cur.execute(qry)

		db.commit()
		db.close()