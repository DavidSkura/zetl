"""
  Dave Skura, 2023
  
"""
from postgresdave_package.postgresdave import postgres_db #install pip install postgresdave-package

import os

try:
	tblname = 'weather.postal_codes'
	mydb = postgres_db()
	mydb.connect()
	print (" Connected " ) # 
	print(mydb.dbversion())

	csvfile = '.\\zetl_scripts\\LoadPostalCodes\\CanadianPostalCodes.csv'
	print(os.getcwd())
	print(csvfile)

except Exception as e:
	print(str(e))
	sys.exit(1)

sys.exit(0)
