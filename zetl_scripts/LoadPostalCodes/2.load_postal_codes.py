"""
  Dave Skura, 2023
  
  File Description:
"""
from postgresdave_package.postgresdave import db #install pip install postgresdave-package

import psycopg2 
import os

tblname = 'weather.postal_codes'
mydb = db()
mydb.connect()
print (" Connected " ) # 
print(mydb.dbversion())

csvfile = '.\\zetl_scripts\\LoadPostalCodes\\CanadianPostalCodes.csv'
print(os.getcwd())
mydb.load_csv_to_table(csvfile,tblname,True,',')
print(csvfile)
