"""
  Dave Skura, Apr 21/2021
"""
import yfinance as yf  
import psycopg2
import sys
from zetl_modules.constants import dbinfo
from flask import Flask, render_template,request,make_response,jsonify, url_for, session

class cols:
	def __init__(self):
		self.columns = []

class metadata_reader():
	def table(self,tbl):
		thesecols = cols()
		thesecols.columns.clear()
		dbconn = psycopg2.connect(user = dbinfo().iuser,
															password = dbinfo().ipwd,
															host = dbinfo().ihost,
															port = dbinfo().iport,
															database = dbinfo().ischema,
															connect_timeout=-1)
		
		dbcur = dbconn.cursor()
		qry = """
				select column_name
				from information_schema.columns
				WHERE table_schema='""" + dbinfo().ischema + """' and table_name='""" + tbl + "'	ORDER BY ordinal_position"
		dbcur.execute(qry)
		data = dbcur.fetchall()
		for row in data:
			thesecols.columns.append(row[0])
		return thesecols

class spark():
	def booleate(self,val):
		if val.upper() == 'TRUE':
			return True
		else:
			return False

	def __init__(self):
		self.version = 2.4
		self.read = metadata_reader()
		self.dbconn = None
		self.dbcur = None
		self.RowsReturned	= None

	def dbdone(self):
		self.dbconn.commit()
		self.dbconn.close()
		self.dbconn = None

	def set_activity_status(self,currently,keyfld):
		usql = "UPDATE activity	SET currently='" + str(currently).replace("'",'') + "', keyfld='" + str(keyfld).replace("'",'') + "',prvkeyfld=keyfld, previously=currently,dtm=CURRENT_TIMESTAMP	"
		self.sql(usql)

	def dbconnect(self):
		self.dbconn = psycopg2.connect(user = dbinfo().iuser,
															password = dbinfo().ipwd,
															host = dbinfo().ihost,
															port = dbinfo().iport,
															database = dbinfo().ischema,
															connect_timeout=-1)

		self.dbcur = self.dbconn.cursor()

	def is_db_on_hold(self):
		dbstatus = self.query_retone('SELECT cast(currently as varchar(250)) FROM Activity')
		if str(dbstatus).find('*hold') > -1:
			print('True')
			return True
		else:
			print('False')
			return False

	def sent_log(self,logmsg):
		#CREATE TABLE sentinel_log (log varchar,dtm timestamp default current_timestamp)
		self.sql("INSERT INTO SENTINEL_LOG (log) VALUES ('" + logmsg + "')")

	def set_db_onhold(self):
		self.sql("UPDATE activity	SET previously=currently,dtm=CURRENT_TIMESTAMP,	prvkeyfld=keyfld, currently='*holdoff*', keyfld=''")

	def set_db_releasehold(self):
		self.sql("UPDATE activity	SET previously=currently,dtm=CURRENT_TIMESTAMP,	prvkeyfld=keyfld, currently='*idle*', keyfld=''")

	def sql(self,qry):
		self.dbconnect()
		self.RowsReturned	= self.dbcur.execute(qry)
		self.dbdone()

	def query_retone(self,qry):
		self.dbconnect()
		self.RowsReturned	= self.dbcur.execute(qry)
		firstfieldvalue	= self.dbcur.fetchone()[0]
		self.dbdone()
		return str(firstfieldvalue)

	def getnxtstock(self):
		getstock_sql = """
									SELECT stock
									FROM (
											SELECT stock,attempted_update_dtm, RANK() OVER(ORDER BY attempted_update_dtm,stock)as rnk
											FROM (
													SELECT stock, max(attempted_update_dtm) as attempted_update_dtm
													FROM stockupdatelist
													group by stock 
													) subqry
									) L
									WHERE rnk=1
									"""
		stock	= self.query_retone(getstock_sql)
		updtstock_sql = "UPDATE stockupdatelist SET attempted_update_dtm=CURRENT_TIMESTAMP WHERE stock = '" + stock + "'"
		isql = "INSERT INTO stockupdatelist_log VALUES ('" + stock + "',CURRENT_TIMESTAMP)"
		self.sql(updtstock_sql)
		self.sql(isql)
		return stock

	def chkdbformaxlogdt(self,stock,default_start_date):

		stockmaxdt_sql = """
					SELECT cast( (max(log_dt) + INTERVAL '1 day') as date) as dcurrent_max_log_dt
					FROM (
						SELECT stock,log_dt
						FROM stockdata
						WHERE Stock='""" + stock+ """'
						UNION	
						SELECT Stock, '""" + default_start_date + """' as log_dt
						FROM stockupdatelist
						WHERE Stock='""" + stock+ """'
						) L
					"""
		return self.query_retone(stockmaxdt_sql)


class cookie():
	def __init__(self,resp):
		self.resp = resp

	#request.cookies.get('username')  
	#cookie().createCookie(prm,val)
	def createCookie(self,prm,val):
		expire_date = datetime.datetime.now()
		expire_date = expire_date + datetime.timedelta(days=90)
		self.resp.set_cookie(prm, value=val,expires=expire_date)

	def chkordfld(self,num):
		sortf = 'sort' + str(num)
		orderv = 'order' + str(num)
		sort_fld = ''
		ord_dirctn = ''
		if sortf in request.args:
			sort_fld = request.args.get(sortf)
			if orderv in request.args:
				ord_dirctn = request.args.get(orderv)
			else:
				ord_dirctn = 'asc'

		return sort_fld,ord_dirctn

	def getorders(self):
		orderbyfields = []
		orderbysorts = []

		for i in range(1,10):
			sort_fld,ord_dirctn = self.chkordfld(i)

			if sort_fld != '':
				orderbyfields.append(sort_fld)
				orderbysorts.append(ord_dirctn)

		zORDERBY = ''
		for i in range(0, len(orderbyfields)):
			zORDERBY += orderbyfields[i] + ' ' + orderbysorts[i] + ','
		if zORDERBY != '':
			zORDERBY = 'ORDER BY ' + zORDERBY[:-1]
		return zORDERBY


class db:
	def __init__(self):
		self.sort1='' #ROI
		self.order1='' #desc
		self.colour_highlow = False
		self.colour_highlowoffset = 0


		self.DB_HOLD = '*holdoff*'
		self.dbconn = None
		self.cur = None
		self.currently = ''
		self.keyfld = ''
		self.prvkeyfld = ''
		self.previously = ''
		self.dtm = ''
		self.status_str = ''
		self.sundayeveningjob_str = ''
		self.sundaynightjob_str = ''
		self.OkToProceed = ''
		self.stockdetails = ''
		self.pstock = ''
		self.linkcols = []
		self.linkcol = '' # 'latest_log_dt'
		self.linkprms = '' # 'log_dt='
		self.linkcolidx = -1
		self.linkpage = ''

		self.linkcolidxs = []
		self.linkonfield = -1

		self.action = ''      #'Delete'
		self.actionpage = ''  #'DeleteStock'
		self.actionfield = '' #'stock'
		self.actionfieldidx = -1 #colidx

		self.stocklist_rowcount = None
		self.stockdata_diststock = None

		self.RowsAffected = None
		self.DataCurrency = None

	def dbdone(self):
		self.dbconn.commit()
		self.dbconn.close()
		self.dbconn = None

	def dbconnect_to_database(self,database):
		self.dbconn = psycopg2.connect(user = dbinfo().iuser,
																password = dbinfo().ipwd,
																host = dbinfo().ihost,
																port = dbinfo().iport,
																database = database,
																connect_timeout=-1)

	def dbconnect(self):
		self.dbconn = psycopg2.connect(user = dbinfo().iuser,
																password = dbinfo().ipwd,
																host = dbinfo().ihost,
																port = dbinfo().iport,
																database = dbinfo().ischema,
																connect_timeout=-1)

		self.cur = self.dbconn.cursor()

	def fetchall(self):
		return self.cur.fetchall()

	def execute(self,sql):
		self.RowsAffected = self.cur.execute(sql)

	def setnextupdate(self,stock):
		self.dbconnect()
		msql = "UPDATE stockupdatelist SET attempted_update_dtm='2000-01-01 00:00:00' WHERE stock='" + stock + "'"
		self.RowsAffected = self.cur.execute(msql)
		self.dbdone()
	def getinfo(self):
		self.dbconnect()
		msql = """
				SELECT C.yearweek, CASE WHEN (wk.oddeven = 1) THEN 'Odd' ELSE 'Even' END as oe, concat('(',min(cal_dt),' - ',max(cal_dt),')')
				FROM Calendar C
					INNER JOIN Calendar_wk wk ON (C.yearweek = wk.yearweek)
				WHERE C.yearweek = (SELECT cast(parametervalue as int) FROM z_control WHERE etl_name='weekly_odds' and parameterkey='Buyweek')
					and dayofweek in (2,3,4,5,6)
				group by C.yearweek, wk.oddeven
				"""
		self.RowsAffected = self.cur.execute(msql)
		data = self.cur.fetchall()
		for row in data:
			yearweek = row[0]
			oe = row[1]
			strt_end = row[2]

		self.dbdone()
		return yearweek, oe, strt_end

	def queryone(self,qry):
		self.RowsAffected = self.cur.execute(qry)
		return self.cur.fetchall()[0][0]

	def query(self,qry):
		self.RowsAffected = self.cur.execute(qry)
		self.data =  self.cur.fetchall()
		return self.data

	def savetofile(self,justfilename,sometext):
		fullfilename =  dbinfo().dir_website_static + justfilename
		f = open(fullfilename,'w')
		f.write(sometext)
		f.close()

	def csvout(self,justfilename):
		fullfilename =  dbinfo().dir_website_static + justfilename
		f = open(fullfilename,'w')
		f.write('rowid')

		for k in [i[0] for i in self.descriptions]:
			f.write(',' + k.lower().strip())
		f.write('\n')

		rowid = 0
		for row in self.data:
			rowid += 1
			f.write(str(rowid))
			for x in range(0,len(self.descriptions)):
				f.write(',' + str(row[x]))
			f.write('\n')
			
		f.close

	def update_and_hold(self,newstatus):
		self.UpdateStatus(self.DB_HOLD + ' ' + newstatus,'Indefinitely')

	def db_hold(self):
		self.UpdateStatus(self.DB_HOLD,'Indefinitely')

	def release_db_hold(self):
		self.UpdateStatus('idle','no key')

	def Ok_To_Proceed(self):
		if self.currently.find(self.DB_HOLD) > -1:
			# this means do run ... another process is hammering the db
			return False
		else:
			return True

	def sql_to_table(self,sql,withcount):
		self.dbconnect()
		self.RowsAffected = self.cur.execute(sql)
		html_table = self.tableout(self.cur,withcount,False,False)
		self.dbdone()
		return html_table

	def UpdateStatus(self,newstatus,keyfld):
		self.dbconnect()
		usql = "UPDATE activity	SET previously=currently,dtm=CURRENT_TIMESTAMP,	prvkeyfld=keyfld, currently='"
		usql += newstatus.replace("'",'').replace('"','') + "', keyfld='" + keyfld.replace("'",'').replace('"','') + "'"
		self.RowsAffected = self.cur.execute(usql)
		self.dbdone()

	def getcolumns(self,cur):
		cols = []
		for k in [i[0] for i in cur.description]:
			cols.append(k)
		return cols

	def htmltableout(self,cur):
		return self.tableout(cur,False,False,False)
	def tableout(self,cur,withcount=False,twonumbers=False,colour_numbers=False):
		#colour_numbers = True
		self.descriptions = cur.description
		self.data = cur.fetchall()
		stockcol = -1
		colidx = 0
		for k in [i[0] for i in self.descriptions]:
			if k.lower().strip() == self.linkcol.lower().strip():
				self.linkcolidx = colidx
			if k.lower().strip() == self.actionfield.lower().strip():
				self.actionfieldidx = colidx

			colidx += 1
		
		trows = ''
		ftockfilter = ''
		if self.pstock != '':
			ftockfilter = 'stock=' + self.pstock + '&'

		prev_cell_value = 0
		rowcount = 0
		colcount = len(self.descriptions)
		rownum = 0
		for row in self.data:
			rownum += 1

		rowcount = rownum
		fricol = self.colour_highlowoffset + 5
		
		rownum = 0
		for row in self.data :

			trows += '<TR class ="normal">'
			for i in range(0,colcount):
				cellvalue = str(row[i])
				
				if self.colour_highlow and i > self.colour_highlowoffset  and cellvalue != 'None':
					zlow = float(cellvalue.split('-')[0])
					zhigh = float(cellvalue.split('-')[1])
					
					if i == (1+self.colour_highlowoffset): 
						if rownum == rowcount:
							prev_cell_value = 0 # since there is no row to compare to
						elif rownum < (rowcount-1):
							if self.data[rownum+1][fricol] != None:
								prev_cell_value = float(self.data[rownum+1][fricol].split('-')[1])  # since there is no row to compare to
							else:
								prev_cell_value = 0
							
					if i <= self.colour_highlowoffset or i > (fricol):
						cellvalue = '<font color=black>' + cellvalue + '</font>'
					else:
						if prev_cell_value > zhigh:
							cellvalue = '<font color=red>' + cellvalue + '</font>'
						elif prev_cell_value == cellvalue:
							cellvalue = '<font color=black>' + cellvalue + '</font>'
						else:
							cellvalue = '<font color=green>' + cellvalue + '</font>'

					prev_cell_value = zhigh

				if twonumbers and i > self.colour_highlowoffset and i <= fricol and cellvalue.find('-') > -1:
					zopen = float(cellvalue.split('-')[0])
					zclose = float(cellvalue.split('-')[1])
					zdiff = str(round(zclose-zopen,1)) 

					if zopen > zclose:
						cellvalue = '<font color=red>' + cellvalue + '</font>'
					elif zopen == zclose:
						cellvalue = '<font color=black>' + cellvalue + '</font>'
					else:
						cellvalue = '<font color=green>' + cellvalue + '</font>'

				if colour_numbers and cellvalue != 'None' and i > 0:
					if float(cellvalue) > 0:
						cellvalue = '<font color=green>' + cellvalue + '</font>'
					elif float(cellvalue) == 0:
						cellvalue = '<font color=black>' + cellvalue + '</font>'
					else:
						cellvalue = '<font color=red>' + cellvalue + '</font>'

				
				if int(i) == int(self.linkcolidx):
					if self.linkpage == '':
						trows += "<TD class=normal><a href=javascript:onclick=reload_withnewstock(this,'" + cellvalue + "')>" + cellvalue + "</a></TD>"	
					else:
						trows += "<TD class=normal><a href='" + self.linkpage + '?' + self.linkprms + cellvalue + "'>" + cellvalue + '</a></TD>'		
				elif int(i) == self.linkonfield:

					lnks = ''
					for j in range(0,len(self.linkcols)):
						lnks += self.linkcols[j] + '=' + str(row[self.linkcolidxs[j]]) + '&'

					if self.sort1 !='': #ROI
						lnks += 'sort1=' + self.sort1 + '&'

					if self.order1 !='': #desc
						lnks += 'order1=' + self.order1


					trows += "<TD class =normal><a href='" + self.linkpage + "?" + lnks + "'>" + cellvalue + "</a></TD>"	
				else:
					trows += '<TD class ="normal">'
					if cellvalue != 'None':

						trows += cellvalue
					trows += '</TD>'

			rownum += 1
	
			if self.action != '':
				trows += "<TD class=normal><a href='" + self.actionpage + '?' + self.actionfield + '=' + str(row[self.actionfieldidx]) + "'>" + self.action + "</TD>"	

			trows += '</TR>'

		hdr = """<TABLE class ="normal"><TR>"""
		onetime = True
		for k in [i[0] for i in self.descriptions]:

			#col = "<INPUT type=checkbox id='" + k + "' name='sortfieldchk' onclick='saveclickstate(this)'> &nbsp"
			col = "<a href=javascript:onclick=reload_reorder(this,'" + k + "')>" + k + "</a>"
			if withcount == False:
				col = '<font color=blue>' + k + '</font>'

			if onetime:
				onetime = False
				if withcount:
					col += '(' + str(rowcount) + ')'

			hdr += '<TH class=normal>' + col + ' </TH>'

		if self.action != '':
			hdr += "<TD align=center><strong> action </strong></TD>"
		hdr + '</TR>'

		ftr = '</TABLE>'
		return hdr + trows + ftr

	def getstatus(self):
		self.dbconnect()
		RowsReturned = self.cur.execute('SELECT currently,previously,dtm,keyfld,prvkeyfld FROM activity')
		data=self.cur.fetchall()
		self.dbdone()
		
		self.currently = data[0][0]
		self.previously = data[0][1]
		self.dtm = data[0][2]
		self.keyfld = data[0][3]
		self.prvkeyfld = data[0][4]

		if self.Ok_To_Proceed():
			self.OkToProceed = ''
		else:
			self.OkToProceed = '<HR>**NOT ** Ok To Proceed.  Another process set status <strong>' + self.DB_HOLD + '</strong>.  Have to wait now.'

		self.stockdetails = ''
		self.dbconnect()

		sql =  " SELECT dtm,log "
		sql += " FROM sentinel_log" 
		sql += " ORDER BY dtm desc limit 10"
		self.RowsAffected = self.cur.execute(sql)
		self.stockdetails = self.tableout(self.cur,False,False,False)
		self.dbdone()

		if self.is_keyfld_a_stock():
			self.dbconnect()
		
			self.RowsAffected = self.cur.execute(sql)
			self.stockdetails = self.tableout(self.cur,False,False,False)

			self.dbdone()
		self.status_str = 'Currently ' + self.currently + ' [' + self.keyfld + '] ' 
		self.status_str += ' since ' + str(self.dtm) + '.  Previously ' + str(self.previously)  + ' [' + self.prvkeyfld + '] '
		self.status_str += self.OkToProceed
		self.status_str += '<HR>' + self.stockdetails



