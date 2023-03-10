"""
  Dave Skura, 2023
"""
import sys

from postgresdave_package.postgresdave import postgres_db #install pip install postgresdave-package

def main():
	if len(sys.argv) == 1 or sys.argv[1] == 'postgres_export.py': # no parameters
		print('usage: ')
		print('postgres_export.py [tablename] [csvfilename] [delimiter] ') 
		print('')

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
		print('postgres_export.py [tablename] [csvfilename] [delimiter] ') 
		print('')

class dbexport():
	def __init__(self,tablename='',csv_filename='',delimiter='\t'):
		self.mydb = postgres_db()
		if csv_filename == '':
			csv_filename=tablename + '.tsv'

		self.mydb.export_table_to_csv(csv_filename,tablename,delimiter)
		self.mydb.close()

		print(tablename + ' exported to ' + csv_filename)


if __name__ == '__main__':
	main()

		

