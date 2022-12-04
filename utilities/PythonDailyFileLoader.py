"""
  Dave Skura


"""
import time
import psycopg2
import os
import sys
from zetl_modules.constants import dbinfo
from zetl_modules.dbxfer import fast_import

print(dbinfo().idescription)

dbconn = psycopg2.connect(user = dbinfo().iuser,
													password = dbinfo().ipwd,
													host = dbinfo().ihost,
													port = dbinfo().iport,
													database = dbinfo().ischema,
													connect_timeout=-1)
dbcur = dbconn.cursor()


foundone = False
datafile_dir = 'data\\daily_delta_files'
datafile_dir_completed = 'data\\daily_delta_files\\loaded'
arr = os.listdir(datafile_dir)

for f in arr:


	fn = datafile_dir + '\\' + f 
	fn_done = datafile_dir_completed + '\\' + f 

	if f.endswith('.txt'):
		foundone = True
		print('loading daily delta ' + fn)

		RowsReturned	= dbcur.execute('DROP TABLE IF EXISTS stock_daily_stooq_tmp')
		dbconn.commit()

		RowsReturned	= dbcur.execute("""
																		CREATE TABLE stock_daily_stooq_tmp (
																			TICKER CHARACTER VARYING, 
																			PER CHARACTER VARYING, 
																			DATE CHARACTER VARYING, 
																			TIME CHARACTER VARYING, 
																			OPEN NUMERIC, 
																			HIGH NUMERIC, 
																			LOW NUMERIC, 
																			CLOSE NUMERIC, 
																			VOL NUMERIC, 
																			OPENINT NUMERIC
																		)
																	""")
		dbconn.commit()

		fast_import().import_file_to_table(fn,'stock_daily_stooq_tmp')

		try:
			os.remove(fn_done)
		except:
			pass
		os.rename(fn,fn_done)

		RowsReturned	= dbcur.execute('DROP TABLE IF EXISTS stock_working_merge_tmp')
		dbconn.commit()

		qry = """
		CREATE TABLE stock_working_merge_tmp as 
		SELECT stooq.* 
		FROM (
					SELECT lower(CASE 
								WHEN SUBSTRING(TICKER,2,1)='.' THEN SUBSTRING(TICKER,1,1)
								WHEN SUBSTRING(TICKER,3,1)='.' THEN SUBSTRING(TICKER,1,2)
								WHEN SUBSTRING(TICKER,4,1)='.' THEN SUBSTRING(TICKER,1,3)
								WHEN SUBSTRING(TICKER,5,1)='.' THEN SUBSTRING(TICKER,1,4)
								WHEN SUBSTRING(TICKER,6,1)='.' THEN SUBSTRING(TICKER,1,5)
								WHEN SUBSTRING(TICKER,7,1)='.' THEN SUBSTRING(TICKER,1,6)
								WHEN SUBSTRING(TICKER,8,1)='.' THEN SUBSTRING(TICKER,1,7)
								WHEN SUBSTRING(TICKER,9,1)='.' THEN SUBSTRING(TICKER,1,8)
								WHEN SUBSTRING(TICKER,10,1)='.' THEN SUBSTRING(TICKER,1,9)
						 ELSE '' 
						 END) as stock, TO_DATE(date,'YYYYMMDD') as log_dt,open,high,low,close,VOL as volume
					FROM stock_daily_stooq_tmp 
					WHERE TICKER like '%US'
				) stooq
				INNER JOIN (SELECT stock as tick FROM stockupdatelist) sul ON (stooq.stock = sul.tick)
		"""

		RowsReturned	= dbcur.execute(qry)
		dbconn.commit()

		qry = """
			DELETE FROM stockdata
			USING stock_working_merge_tmp SWM
			WHERE	stockdata.stock = SWM.stock and 
					stockdata.log_dt = SWM.log_dt
			"""

		RowsReturned	= dbcur.execute(qry)
		dbconn.commit()

		qry = """
			INSERT INTO stockdata (stock,log_dt,open,high,low,close,volume)
			SELECT SWM.stock,SWM.log_dt
				,max(SWM.open) as open
				,max(SWM.high) as high
				,max(SWM.low) as low
				,max(SWM.close) as close
				,max(SWM.volume) as volume
			FROM stock_working_merge_tmp SWM
			GROUP BY SWM.stock,SWM.log_dt
			"""

		RowsReturned	= dbcur.execute(qry)
		dbconn.commit()

		RowsReturned	= dbcur.execute('DROP TABLE stock_daily_stooq_tmp')
		dbconn.commit()
		RowsReturned	= dbcur.execute('DROP TABLE IF EXISTS stock_working_merge_tmp')
		dbconn.commit()


if not foundone:
	print('no *.txt files to load in ' + datafile_dir)

time.sleep(5) 
