"""
  Dave Skura, 2021
  
  File Description:
"""
from postgresdatabase import db
import psycopg2 
import os

tblname = 'weather.postal_codes'
mydb = db()
mydb.connect()
print (" Connecting " + mydb.connection_str) # 
print(mydb.get_dbversion())

csvfile = 'CanadianPostalCodes.csv'
mydb.load_csv_to_table(csvfile,tblname,False,',')
print(csvfile)
