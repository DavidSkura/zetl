"""
  Dave Skura, Dec,2022
"""
from zetl.run import zetl

import warnings
import sys
import os
import re
from datetime import *

my_zetl = zetl()

def main():
	if len(sys.argv) == 1 or sys.argv[1] == 'view.py': # no parameters
		print('')
		print('usage: ')
		print('py -m zetl.view [etl_name] ') 
		print('-----------')
		my_zetl.zetldb.empty_zetl()						# empty the master zetl table
		my_zetl.zetldb.load_folders_to_zetl() # load master zetl table from folders
		my_zetl.show_etl_name_list()

	else: # run the etl match the etl_name in the etl table
		etl_name_to_run = sys.argv[1]
		showetl(etl_name_to_run)

def showetl(etl_name_to_run):
	try:
		my_zetl.zetldb.empty_zetl(etl_name_to_run)
		my_zetl.zetldb.load_folders_to_zetl(etl_name_to_run)
		my_zetl.zetldb.export_zetl(etl_name_to_run)
		f = open('zetl_scripts/' + etl_name_to_run + '/z_etl.csv','r')
		content = f.read()
		print('')
		print('zetl: ' +  etl_name_to_run)
		print('folder: ' + './zetl_scripts/' + etl_name_to_run)
		print('-----------')
		print(content)
		f.close()
	except:
		print(etl_name_to_run + ' does not exist in ./zetl_scripts')

if __name__ == '__main__':
	#showetl('test')
	main()