"""
  Dave Skura, 2023
  
"""

from postgresdave_package.postgresdave import db #install pip install postgresdave-package

mydb = db()
mydb.useConnectionDetails('postgres','<your-password-here>','localhost','1532','postgres','public')

csv_filename='sample.tsv'
tablename='sampletable'

mydb.export_table_to_csv(csv_filename,tablename,szdelimiter='\t')
mydb.close()
