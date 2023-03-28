"""
  Dave Skura,2023
"""
import sys
from schemawizard_package.schemawizard import schemawiz

def main():
	if len(sys.argv) == 1 or sys.argv[1] == 'sqlite_import.py': # no parameters
		print('usage: \n')
		print('sqlite_import.py [csv_filename] [tablename] [WithTruncate]') 
		print('')

		print("\t - text csv_filename = 'anyfilename.csv'")
		print("\t - text tablename = 'anytablename'")
		print('\t - bool WithTruncate = [True,False]\n')

	elif (len(sys.argv) == 2) : 
		csv_filename = sys.argv[1]
		myexp = dbimport(csv_filename)

	elif (len(sys.argv) == 3) : 
		csv_filename = sys.argv[1]
		tablename = sys.argv[2]

		myexp = dbimport(csv_filename,tablename)

	elif (len(sys.argv) == 4) : 
		csv_filename = sys.argv[1]
		tablename = sys.argv[2]
		WithTruncate = bool(sys.argv[2])

		myexp = dbimport(csv_filename,tablename,WithTruncate)

	else:
		print('Error: Incorrect number of parameters.')
		print('')
		print('usage: ')
		print('sqlite_import.py [csv_filename] [tablename] [WithTruncate]') 
		print('')
		print('bool WithTruncate = [True,False]')

class dbimport():
	def __init__(self,csv_filename,tablename='',WithTruncate=False):
		tbl = tablename
		dber = schemawiz()
		if ((tablename != '') and (dber.dbthings.sqlite_db.does_table_exist(tablename))):
			dber.justload_sqlite_from_csv(csv_filename,tablename,WithTruncate)
		else:
			output_ddl_filename = 'z.' + self.getoutputfilename(csv_filename,tablename) + '.ddl'
			tbl = dber.createload_sqlite_from_csv(csv_filename,tablename,output_ddl_filename)

		dber.dbthings.sqlite_db.close()

		print(csv_filename + ' imported to ' + tbl)

	def getoutputfilename(self,csv_filename,ddl_output_filename):
		# zetl_scripts/someetl/thisfile.csv
		filedelimiter = '\\'
		if csv_filename.find('/') > -1:
			filedelimiter = '/'

		newfile = ''
		partlist = csv_filename.split(filedelimiter)
		if partlist[0] != csv_filename:
			for i in range(0,len(partlist)-1):
				newfile += partlist[i] + filedelimiter

		newfile += ddl_output_filename
		return newfile

if __name__ == '__main__':
	#myexp = dbimport('sample.csv','thistbl',False)

	#tbl = schemawiz().createload_sqlite_from_csv('sample.csv','thistbl')
	#data = schemawiz().dbthings.sqlite_db.query('SELECT * FROM thistbl')
	#for row in data:
	#	print(row)

	main()

		