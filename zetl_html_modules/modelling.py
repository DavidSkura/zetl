"""
  Dave Skura, 2021
"""
import subprocess
from flask import Flask, render_template,request,make_response,jsonify, url_for, session

from zetl_modules.zetl_utilities import db
from zetl_html_modules.admin import useradmin
import datetime

class modelling:
	def __init__(self):
		self.sqldb = db()

	def modellingpage(self):

		result_html = '' 
		self.sqldb.dbconnect()

		csql = ''
		stock = ''
		log_dt = ''
		refresh = ''
		fit_label = ''

		if 'stock' in request.args:
			stock = request.args.get('stock')
		if 'log_dt' in request.args:
			log_dt = request.args.get('log_dt')
		if 'refresh' in request.args:
			refresh = request.args.get('refresh')

		asql = self.getsql_avg_fit_label(stock,log_dt)
		gsql = self.getsql_labels(stock,log_dt)
		chkmodelexists_sql = self.getsql_chkmodelexists(stock,log_dt)

		ModelExists = False
		if stock != '' and log_dt != '':
			if refresh != '':
				self.sqldb.execute(self.getsql_delete_model(stock,log_dt))
				self.sqldb.dbconn.commit()

			edata = self.sqldb.query(chkmodelexists_sql)
			if int(edata[0][0]) == 0:
				imsg = 'Model not found.'
				maxminsdata_v = self.sqldb.queryone(self.getsql_maxminutedata(stock,log_dt))
				if maxminsdata_v:
					maxminsdata = int(maxminsdata_v)
				else:
					maxminsdata = 0

				if maxminsdata > 1530 or not self.chk_is_today(log_dt):
					gensql = 'call fitstockmodel_day( '
				elif maxminsdata > 1430:
					gensql = 'call fitstockmodel_1530( '
				elif maxminsdata > 1330:
					gensql = 'call fitstockmodel_1430( '
				elif maxminsdata > 1230:
					gensql = 'call fitstockmodel_1330( '
				elif maxminsdata > 1130:
					gensql = 'call fitstockmodel_1230( '
				elif maxminsdata > 1030:
					gensql = 'call fitstockmodel_1130( '
				else:
					gensql = 'call fitstockmodel_1030( '

				self.sqldb.execute(gensql + "'" + stock + "','" + log_dt + "')")

				self.sqldb.dbconn.commit()
				imsg += '  Tried creating model.'
				adata = self.sqldb.query(chkmodelexists_sql + ' ')
				if int(adata[0][0]) == 0:
					ModelExists = False
					imsg += '  Create model failed check stockdata_minute_hist'
				else:
					ModelExists = True
					imsg += '  Model Created. ' + str(adata[0][0]) 
			
			else:
				ModelExists = True
				imsg = 'Model found.'

			#print(imsg)
			avg_trading_at = '0'
			if ModelExists:
				bdata = self.sqldb.query(asql)
				avg_trading_at = bdata[0][0]
				fit_label = bdata[0][1]

				cdata = self.sqldb.query(gsql)
				model_names=[]
				for row in cdata:
					model_names.append(row[0])

				stock_lbl = stock.upper() + ' avg()=' + str(avg_trading_at)
				csql = self.getsql_chartdata(stock,log_dt,model_names,stock_lbl)
			
		result_html += """<FORM action="modelling" method = "GET" enctype = "multipart/form-data">"""
		result_html += "<label for='stock'>Stock</label> <INPUT name='stock' id='stock' size=5 value='" + stock + "'>  &nbsp|&nbsp "
		result_html += "<label for='log_dt'>log_dt</label> <INPUT name='log_dt' id='log_dt' size=10 value='" + log_dt + "'>  &nbsp|&nbsp "

		result_html += "<INPUT type=submit id=refresh name=refresh value=Refresh> &nbsp|&nbsp <INPUT type=submit id=submit name=submit value=Search> </FORM>"

		self.sqldb.savetofile('chartdata.sql',csql)
		self.sqldb.dbdone()

		return result_html,csql,fit_label

	def getsql_avg_fit_label(self,stock,log_dt):
		asql = """
						SELECT DISTINCT avg_trading_at,fit_label
						FROM latest_model_fit
						WHERE stock='tt' and log_dt = '2021-04-22'
		"""
		asql = asql.replace('tt',stock)
		asql = asql.replace('2021-04-22',log_dt)
		return asql

	def getsql_labels(self,stock,log_dt):

		gsql = """
			SELECT label
			FROM (
					SELECT concat(fit_stock,':',fit_log_dt,':',total_variance) as label
							,RANK() OVER (PARTITION BY Stock ORDER BY total_variance,fit_stock,fit_log_dt ) var_rnk
					FROM latest_model_fit
					WHERE stock='tt' and log_dt = '2021-04-22'
					) L
			WHERE var_rnk < 4
		"""
		gsql = gsql.replace('tt',stock)
		gsql = gsql.replace('2021-04-22',log_dt)
		return gsql

	def getsql_chkmodelexists(self,stock,log_dt):
		chkmodelexists_sql = """
				SELECT count(*)
				FROM latest_model_fit
				WHERE stock='tt' and log_dt='2021-04-22'
		"""
		chkmodelexists_sql = chkmodelexists_sql.replace('tt',stock)
		chkmodelexists_sql = chkmodelexists_sql.replace('2021-04-22',log_dt)
		return chkmodelexists_sql

	def getsql_delete_model(self,stock,log_dt):
		sql="""
			DELETE
			FROM latest_model_fit
			WHERE stock = 'tt' and log_dt = '2021-04-22'
		"""

		sql = sql.replace('tt',stock)
		sql = sql.replace('2021-04-22',log_dt)
		return sql

	def chk_is_today(self,log_dt):
		current_date = datetime.date.today()
		szcurrent_date = current_date.strftime("%Y-%m-%d")
		if szcurrent_date == log_dt:
			return True
		else:
			return False

	def getsql_maxminutedata(self,stock,log_dt):
		sql = """
			SELECT max(hrs)*100 + max(mins) as maxminutedata
			FROM StockLog_dt_factors
			WHERE stock= 'tt' AND log_dt= '2021-04-22' 
		"""
		sql = sql.replace('tt',stock)
		sql = sql.replace('2021-04-22',log_dt)
		return sql

	def getsql_chartdata(self,stock,log_dt,model_names,stock_lbl):
		csql="""
		WITH CTE as (
					SELECT var_rnk,stock,log_dt,fit_stock,fit_log_dt,model_name
					FROM (
							SELECT stock,log_dt,fit_stock,fit_log_dt
									,concat(fit_stock,':',fit_log_dt,':',total_variance) as model_name
									,RANK() OVER (PARTITION BY Stock ORDER BY total_variance,fit_stock,fit_log_dt ) var_rnk
									
							FROM latest_model_fit
							WHERE stock='tt' and log_dt='2021-04-22' 
							) L
					WHERE var_rnk < 4
					ORDER BY var_rnk    

				)

				SELECT 
						round((hrs::numeric + mins::numeric/100),2) as time
						,round(avg_trading_at_1value + (SDF.factor/100 * avg_trading_at_1value),2) as "selected stock"
						,round(avg_trading_at_1value + (DM1.factor/100 * avg_trading_at_1value),2) as "model1"
						,round(avg_trading_at_1value + (DM2.factor/100 * avg_trading_at_1value),2) as "model2"
						,round(avg_trading_at_1value + (DM3.factor/100 * avg_trading_at_1value),2) as "model3"

				FROM 
						day_model_factors_day DM1
						LEFT JOIN (
												SELECT *
												FROM StockLog_dt_factors
												WHERE stock= 'tt' AND log_dt= '2021-04-22'                     
						"""

		if self.chk_is_today(log_dt):
			csql += " AND  hrs::integer < extract(hour FROM CURRENT_TIMESTAMP)::integer "

		csql += """
		
										 ) SDF USING (hrs,mins)

						INNER JOIN CTE CTE1 ON (DM1.stock = CTE1.fit_stock AND DM1.log_dt = CTE1.fit_log_dt AND CTE1.var_rnk=1)
						
						INNER JOIN day_model_factors_day DM2 USING (hrs,mins)
						INNER JOIN CTE CTE2 ON (DM2.stock = CTE2.fit_stock AND DM2.log_dt = CTE2.fit_log_dt AND CTE2.var_rnk=2)
					
						INNER JOIN day_model_factors_day DM3 USING (hrs,mins)
						INNER JOIN CTE CTE3 ON (DM3.stock = CTE3.fit_stock AND DM3.log_dt = CTE3.fit_log_dt AND CTE3.var_rnk=3)
						,(  SELECT max(avg_trading_at) as avg_trading_at_1value
                            FROM StockLog_dt_factors
                            WHERE stock= 'tt' AND log_dt= '2021-04-22'                     
        						  ) avgt							
				ORDER BY time
		"""
		csql = csql.replace('tt',stock)
		csql = csql.replace('2021-04-22',log_dt)
		csql = csql.replace('model1',model_names[0])
		csql = csql.replace('model2',model_names[1])
		csql = csql.replace('model3',model_names[2])
		csql = csql.replace('selected stock',stock_lbl)

		return csql
