"""
  Dave Skura


"""
import time
import psycopg2
import os
import sys
from zetl_modules.constants import dbinfo
from zetl_modules.dbxfer import fast_import

def checkfilehdr(pfilename):
	default_imode = 0
	f = open(pfilename,'r')
	hdr = f.readline()
	line1 = f.readline()
	
	if line1.split(',')[3]=='000000':
		default_imode = 0
	else:
		default_imode = 2

	f.close()
	return default_imode

datafile_dir = 'data\\minute_delta_files'
datafile_dir_completed = datafile_dir + '\\loaded'
tmpcsvfile = datafile_dir + '\\current_stock_minutes_tmp.csv'
arr = os.listdir(datafile_dir)

print(dbinfo().idescription)

dbconn = psycopg2.connect(user = dbinfo().iuser,
													password = dbinfo().ipwd,
													host = dbinfo().ihost,
													port = dbinfo().iport,
													database = dbinfo().ischema,
													connect_timeout=-1)
dbcur = dbconn.cursor()



RowsReturned	= dbcur.execute('DROP TABLE IF EXISTS current_stock_minutes_tmp')
dbconn.commit()
RowsReturned	= dbcur.execute('DROP TABLE IF EXISTS stock_minute_stooq_tmp')
dbconn.commit()

foundone = False
for f in arr:
	fn = datafile_dir + '\\' + f 
	fn_done = datafile_dir_completed + '\\' + f 

	if f.endswith('.txt'):
		foundone = True

		RowsReturned	= dbcur.execute("""
																			CREATE TABLE stock_minute_stooq_tmp (
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

		print('loading minute delta  ' + fn)
		fast_import().import_file_to_table(fn,'stock_minute_stooq_tmp')
		try:
			os.remove(fn_done)
		except:
			pass
		os.rename(fn,fn_done)

		dbcur.execute("SELECT count(DISTINCT TICKER) FROM stock_minute_stooq_tmp WHERE TICKER like '%US' ")
		stock_count = dbcur.fetchone()[0]
		if stock_count == 0:
			print('0 stocks to load.')
			sys.exit()
		else:
			print(str(stock_count) + ' stocks to load')

			# get the data and fill the gaps
			sql = """
			SELECT A.stock
					,log_dt
					,log_dt + (hrs * interval '1 hour')  + (mins * interval '1 minute') as log_dtm
					,open
					,high
					,low
					,close
					,volume
					,hrs
					,mins
			FROM (
							SELECT stock,log_dt,hrs,mins
							FROM DaySlices DS -- 78
											,(SELECT stock,log_dt 
											FROM (  SELECT lower(CASE 
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
																							 END) as stock,TO_DATE(DATE,'YYYYMMDD') as log_dt
																			FROM stock_minute_stooq_tmp 
																			WHERE TICKER like '%US' 
															 ) tmp
											GROUP BY stock,log_dt)ld -- 36 ..... 78*36 = 2808
										
							GROUP BY stock,log_dt,hrs,mins
							) A
							LEFT JOIN (
											SELECT *
											FROM (
															SELECT   lower(CASE 
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
																							 END) as stock
																			 ,TO_DATE(DATE,'YYYYMMDD') as log_dt
																			 ,TO_TIMESTAMP(concat(DATE,time),'YYYYMMDDHH24MISS') as log_dtm
																			 ,open,high,low,close,vol as volume
																			,(extract(hour FROM TO_TIMESTAMP(concat(DATE,time),'YYYYMMDDHH24MISS'))-6)::integer as hrs
																			,extract(minute FROM TO_TIMESTAMP(concat(DATE,time),'YYYYMMDDHH24MISS'))::integer as mins
															FROM stock_minute_stooq_tmp 
															WHERE TICKER like '%US' 
															) tmp2
											) smh USING (stock,log_dt,hrs,mins)
			 ORDER BY stock,A.log_dt,A.hrs,A.mins		
			"""
			RowsReturned	= dbcur.execute(sql)
			data = dbcur.fetchall()
			fh = open(tmpcsvfile,'w')
			fh.write('stock,log_dt,log_dtm,open,high,low,close,volume,hrs,mins\n')

			stock=''
			log_dt = ''
			log_dtm = ''
			hrs = ''
			mins = ''
			fopen = ''
			high = ''
			low = ''
			close = ''
			volume = ''
			newstock = True
			firstrow = True
			for row in data:
				if stock != row[0]:
					newstock = True
					stock = row[0]
				else:
					newstock = False

				log_dt = row[1]
				log_dtm = row[2]
				hrs = row[8]
				mins = row[9]
				if firstrow:
					firstrow = False
				
				if newstock:
					fopen = ''
					high = ''
					low = ''
					close = ''
					volume = ''

				if row[3]: # only assign new values if row is NOT a NULL row
					fopen = row[3]
					high = row[4]
					low = row[5]
					close = row[6]
					volume = row[7]

				fh.write(str(stock) + ',')
				fh.write(str(log_dt) + ',')
				fh.write(str(log_dtm) + ',')
				fh.write(str(fopen) + ',')
				fh.write(str(high) + ',')
				fh.write(str(low) + ',')
				fh.write(str(close) + ',')
				fh.write(str(volume) + ',')
				fh.write(str(hrs) + ',')
				fh.write(str(mins))
				fh.write('\n')

			fh.close()

			RowsReturned	= dbcur.execute("""
																			CREATE TABLE current_stock_minutes_tmp (
																				stock CHARACTER VARYING, 
																				log_dt DATE, 
																				log_dtm CHARACTER VARYING, 
																				open NUMERIC, 
																				high NUMERIC, 
																				low NUMERIC, 
																				close NUMERIC, 
																				volume NUMERIC, 
																				hrs NUMERIC, 
																				mins NUMERIC,
																				PRIMARY KEY(stock,log_dt,log_dtm)
																			)
																		""")
			dbconn.commit()

	 
			fast_import().import_file_to_table(tmpcsvfile,'current_stock_minutes_tmp')
			dbconn.commit()


			# patch starting nulls
			qry = """
					UPDATE current_stock_minutes_tmp
					SET open=A.open,
									high=A.high,
									low=A.low,
									close=A.close,
									volume=A.volume
					FROM (SELECT stock, log_dt, min(open) as open
																	,min(high) as high
																	,min(low) as low
																	,min(close) as close
																	,min(volume) as volume
								FROM current_stock_minutes_tmp
								GROUP BY stock,log_dt) A
					WHERE current_stock_minutes_tmp.stock = A.stock and 
							current_stock_minutes_tmp.log_dt = A.log_dt and
							current_stock_minutes_tmp.open is null;
			"""
			RowsReturned	= dbcur.execute(qry)
			dbconn.commit()

			qry = """
				DELETE 
				FROM stockdata_minute
				USING current_stock_minutes_tmp SSM
				WHERE	stockdata_minute.stock = SSM.stock and 
						stockdata_minute.log_dt = SSM.log_dt
				"""

			RowsReturned	= dbcur.execute(qry)
			dbconn.commit()
		

			qry = """
				INSERT INTO stockdata_minute(stock,log_dt,log_dtm,open,high,low,close,volume)
				SELECT stock,log_dt::date,log_dtm::timestamp,open,high,low,close,volume
				FROM current_stock_minutes_tmp stooq
						INNER JOIN (SELECT stock as tick FROM stockupdatelist) sul ON (stooq.stock = sul.tick)
				"""

			RowsReturned	= dbcur.execute(qry)
			dbconn.commit()


			RowsReturned	= dbcur.execute('DROP TABLE current_stock_minutes_tmp')
			dbconn.commit()
			RowsReturned	= dbcur.execute('DROP TABLE stock_minute_stooq_tmp')
			dbconn.commit()



time.sleep(5) 
