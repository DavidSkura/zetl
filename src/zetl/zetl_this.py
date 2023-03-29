"""
  Dave Skura, Mar,2023
"""
from run import zetl

import warnings
import sys
import os
import re
from datetime import *

my_zetl = zetl()

def main():
	if len(sys.argv) == 1 or sys.argv[1] == 'zetl_this.py': # no parameters
		print('')
		print('usage: ')
		print('py -m zetl.zetl_this [folder] ') 
		print('-----------')

	else: # run the etl match the etl_name in the etl table
		folder = sys.argv[1]
		my_zetl.zetldb.empty_zetl()						# empty the master zetl table
		my_zetl.zetldb.load_thisfolder_to_zetl(folder) # 'f:\git\helloworld'
		showetl(folder)

def showetl(folder):
	try:
		my_zetl.proper_folder_run(folder)
	except Exception as e:
		print(str(e))

if __name__ == '__main__':
	main()