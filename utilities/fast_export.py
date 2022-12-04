"""
  Dave Skura
  
  File Description:fast_export
	
	creates a file like data_20190613.csv
	from input table.

	like...
	
	COPY stock_minute_hist TO 'c:\fastload\stock_minute_hist.csv' DELIMITER ',' CSV HEADER



"""
import os
import psycopg2
from datetime import *

now = (datetime.now())

input_table='stock_minute_hist'


db = psycopg2.connect(user = "dad",
													password = "dad",
													host = "192.168.0.100",
													port = "5432",
													database = "data",
													connect_timeout=-1)

cur = db.cursor()
cur.itersize = 1000 # chunk size

csv_filename='data//initial_loads//stock_minute_hist.csv'
sql = "SELECT * FROM " + input_table  
	
cur.execute(sql)
f = open(csv_filename,'w')
sz = ''
for k in [i[0] for i in cur.description]:
	sz += k + ','
f.write(sz[:-1] + '\n')

for row in cur:
	sz = ''
	for i in range(0,len(cur.description)):
		sz += str(row[i])+ ','

	f.write(sz[:-1] + '\n')

f.close()
