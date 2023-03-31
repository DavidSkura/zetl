"""
  Dave Skura, Mar,2023
"""
from zetl.run import zetl

import warnings
import sys
import os
import re
from datetime import *

def main():
	if len(sys.argv) == 1 or sys.argv[1] == 'zetl_this_file.py': # no parameters
		print('')
		print('usage: ')
		print('py -m zetl.zetl_this_file [file] ') 
		print('-----------')

		#my_zetl.proper_file_run('1.CURRENT_DATE.sql')

	else: # run the etl match the etl_name in the etl table
		filename = sys.argv[1]
		zetl().proper_file_run(filename)

if __name__ == '__main__':
	main()