"""
  Dave Skura,2023

"""

from postgresdave_package.postgresdave import db #install pip install postgresdave-package

mydb = db()
mydb.useConnectionDetails('postgres','<your-password-here>','localhost','1532','postgres','public')

csv_filename='sample.csv'
tablename='sampletable'

mydb.load_csv_to_table(csv_filename,tablename,withtruncate=True,szdelimiter=',')
print(mydb.queryone('SELECT COUNT(*) FROM ' + tablename))
mydb.close()
