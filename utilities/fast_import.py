"""
  Dave Skura
  
  File Description:fast_import

	COPY stockdata_hist FROM 'D:\zetl\data\initial_loads\stockdata_hist.csv' ( FORMAT CSV, DELIMITER(','),HEADER )

"""
import sys
import mysql.connector
from mysqldatabase import db
from zetl_config import variables
samplecall = """
fast_import.py data\\initial_loads\\stockdata_2021.csv stockdata no_truncate \n
fast_import.py data\\initial_loads\\stockdata_2020.csv stockdata truncate \n
	"""
if len(sys.argv) == 1 or sys.argv[1] == 'fast_import.py':
	action		= 'run without parameters. Using defaults \n'
	input_file= variables().dir_zetl_install + 'data\\initial_loads\\stockdata_2021.csv'
	tablename	= 'stockdata'
	option = 'truncate'
elif len(sys.argv) == 2: # 1 parms
	print('1 parameter passed, need 3. \n')
	print(samplecall)
	sys.exit(0)
elif len(sys.argv) == 3: # 2 parms
	print('2 parameters passed, need 3. \n')
	print(samplecall)
	sys.exit(0)

elif len(sys.argv) == 4: # 3 parms
	action		= 'Using parameters. \n'
	input_file = variables().dir_zetl_install + sys.argv[1]
	tablename	= sys.argv[2]
	option	= sys.argv[3]
else:
	print('too many parms. \n')
	sys.exit(0)

action		+= 'Importing ' + input_file + ' to ' + variables().ihost + ':' + variables().ischema + '.' + tablename + ' with ' + option

print(action)

szdelimiter=','
mydb = db()

db1 = mydb.connect()

cur = db1.cursor()

if option.lower() == 'truncate':
	cur.execute('TRUNCATE TABLE ' + tablename)

f = open(input_file,'r')
hdrs = f.read(1000).split('\n')[0].strip().split(szdelimiter)
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
				if row[j].lower() == 'none' or row[j].lower() == 'null':
					newline += "NULL,"
				else:
					newline += "'" + row[j].replace(',','').replace("'",'') + "',"
				
			ilines += newline[:-1] + '),'
			
			if batchcount > 500:
				qry = isqlhdr + ilines[:-1]
				batchcount = 0
				ilines = ''
				cur.execute(qry)
				db1.commit()

if batchcount > 0:
	qry = isqlhdr + ilines[:-1]
	batchcount = 0
	ilines = ''
	cur.execute(qry)

db1.commit()
db1.close()