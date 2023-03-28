"""
  Dave Skura, 2023
"""
import sys

from sqlitedave_package.sqlitedave import sqlite_db #install pip install sqlitedave_package

def main():
	if len(sys.argv) == 1 or sys.argv[1] == 'sqlite_export.py': # no parameters
		print('usage: ')
		print('sqlite_export.py [tablename] [csvfilename] [delimiter] ') 
		print('')

		print("\t - text tablename = 'anytablename'")
		print("\t - text csv_filename = 'anyfilename.csv'")
		print("\t - char delimiter (tab is default) = 'any_single_char'\n")

	elif (len(sys.argv) == 2) : 
		tablename = sys.argv[1]
		myexp = dbexport(tablename)

	elif (len(sys.argv) == 3) : 
		tablename = sys.argv[1]
		csv_filename = sys.argv[2]
		myexp = dbexport(tablename,csv_filename)

	elif (len(sys.argv) == 4) : 
		tablename = sys.argv[1]
		csv_filename = sys.argv[2]
		delimiter = sys.argv[3]
		myexp = dbexport(tablename,csv_filename,delimiter)

	else:
		print('Error: Incorrect number of parameters.')
		print('')
		print('usage: ')
		print('sqlite_export.py [tablename] [csvfilename] [delimiter] ') 
		print('')

class dbexport():
	def __init__(self,tablename='',csv_filename='',delimiter='\t'):
		self.mydb = sqlite_db()
		if csv_filename == '':
			csv_filename=tablename + '.tsv'

		self.mydb.export_table_to_csv(csv_filename,tablename,delimiter)
		self.mydb.close()

		print(tablename + ' exported to ' + csv_filename)

if __name__ == '__main__':
	#myexp = dbexport('tabletblcsv_202303281056','tabletblcsv_202303281056.csv')
	
	main()

		

