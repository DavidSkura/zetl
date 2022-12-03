"""
  Dave Skura, 2021
"""
from flask import Flask, render_template,request,make_response,jsonify, url_for, session
from zetl_modules.zetl_utilities import db,cols,metadata_reader,spark
from zetl_modules.constants import dbinfo,sentinel_constants
from zetl_html_modules.admin import useradmin
import simplejson as json


# ***********************************************************************************
# Sentinel
# ***********************************************************************************

class sentinel_server:
	def __init__(self):
		self.SENTINEL_SERVER_VERSION = 4.0

		self.IncludeRecommend				= True
		self.IncludeData						= True
		self.Includetherest					= True
		self.reset_stockdata				= True
		self.Stockdata_Valid_Until	= '2021-03-09' # data newer than this will be replaced.

		self.sqldb = db()
		self.constant=sentinel_constants()
		self.constant.init(self.IncludeRecommend,self.IncludeData,self.Includetherest)

	def stkdta(self):

		retval = self.constant.error_return_code
		json_hr = json_handler()
		Errormsg = ''
		stock = ''
		mxlogdt = ''
		
		#try:
		if 'Errormsg' in request.args:
			Errormsg = request.args.get('Errormsg')
		if 'stock' in request.args:
			stock = request.args.get('stock')
		if 'mxlogdt' in request.args:
			mxlogdt = request.args.get('mxlogdt')
		
		if Errormsg != '':
			iSQL = "INSERT INTO delisted(stock,error) VALUES ('" + stock + "','" + Errormsg.replace("'",'') + "')"
			spark().sql(iSQL)
		else:
			json_hr.load_request_json(request.json)
			dSQL = "DELETE FROM stockdata WHERE stock = '" + stock + "' and log_dt >= '" + mxlogdt + "'"
			spark().sql(dSQL)
			json_hr.insert_to_stockdata(stock)

			retval = self.constant.success_return_code
		#except Exception as e:
		#	retval = self.constant.error_return_code + ': ' +  str(e) 
	
		return retval

	def action(self):

		isql = """
		SELECT (DATE_PART('day', max(attempted_update_dtm) - min(attempted_update_dtm)) * 24 * 60 + 
					 DATE_PART('hour', max(attempted_update_dtm) - min(attempted_update_dtm))) * 60 +
					 DATE_PART('minute', max(attempted_update_dtm) - min(attempted_update_dtm)) as minute_diff
		FROM stockupdatelist
		"""
		updatelist_minute_diff = float(spark().query_retone(isql))
		#print('\n\tupdatelist_minute_diff = ' + str(updatelist_minute_diff) + '\n')
		dbstatus = str(spark().query_retone('SELECT currently FROM Activity'))
		stock_count = float(spark().query_retone("SELECT parameter1 FROM etl_running WHERE etl_name='sentinel' and status='stock_count'"))
		if stock_count < 1:
			dbstatus='done all *hold'

		SENTINEL_ID = '**no sentinel id found**'
		if 'SENTINEL_ID' in request.form:
			SENTINEL_ID = request.form.get('SENTINEL_ID')


		if dbstatus.find('*hold') > -1 or updatelist_minute_diff < 1000: # stop client from running
			jsonreply = '{'
			jsonreply += '"API_KEY":"' + self.constant.validated + '",'
			jsonreply += '"ACTION":"db_on_hold",'
			jsonreply += '"stock":"",'
			jsonreply += '"maxlogdt":""'
			jsonreply += '}' 
			# ******* LOG BEGIN ******* 
			lmsg = 'www.py' + ': '
			lmsg += 'SENTINEL_ID=' + str(SENTINEL_ID) + ','
			lmsg += 'ACTION=*hold'
			spark().sent_log(lmsg) 
			# ******* LOG END ******* 


			return jsonreply
		elif not self.constant.CheckaStock:
			try:
				if request.form.get('API_KEY') == self.constant.API_KEY:
					if request.form.get('ACTION') == self.constant.ACTION_NewWorkAction:
						jsonreply = '{'
						jsonreply += '"API_KEY":"' + self.constant.validated + '",'
						jsonreply += '"ACTION":"' + self.constant.ACTION_AssignNewWork + '",'
						jsonreply += '"WorkType":"' + self.constant.ACTION_WorkType + '",'
						jsonreply += '"stock":"' + 'Nothing' + '",'
						jsonreply += '"maxlogdt":"' + '1min' + '"'
						jsonreply += '}' 

						# ******* LOG BEGIN ******* 
						lmsg = 'www.py' + ': '
						lmsg += 'SENTINEL_ID=' + str(SENTINEL_ID) + ','
						lmsg += 'ACTION=' + str(self.constant.ACTION_AssignNewWork) + ','
						lmsg += 'ACTION_WorkType=' + str(self.constant.ACTION_WorkType)
						spark().sent_log(lmsg) 
						# ******* LOG END ******* 

						return jsonreply

			except Exception as e:
				return self.constant.action_error_return_code + ':' + str(e) # 

		else:
			try:
				if request.form.get('API_KEY') == self.constant.API_KEY:
					if request.form.get('ACTION') == self.constant.ACTION_NewWorkAction:
						stock = spark().getnxtstock()

						stock_count = stock_count - 1
						spark().sql("UPDATE etl_running SET parameter1 = parameter1::integer - 1 WHERE etl_name = 'sentinel' and status='stock_count'")

						default_start_date = '2019-01-01'

						
						if self.IncludeData and self.reset_stockdata:
							default_start_date = self.Stockdata_Valid_Until
							spark().sql("DELETE FROM stockdata WHERE stock='" + stock + "' and log_dt > '" + self.Stockdata_Valid_Until + "'")

						maxlogdt = spark().chkdbformaxlogdt(stock,default_start_date)
						spark().set_activity_status(SENTINEL_ID,stock)

						# ******* LOG BEGIN ******* 
						lmsg = 'www.py' + ': '
						lmsg += 'SENTINEL_ID=' + str(SENTINEL_ID) + ','
						lmsg += 'stock=' + str(stock) + ','
						lmsg += 'maxlogdt=' + str(maxlogdt) + ','
						lmsg += 'ACTION=' + str(self.constant.ACTION_AssignNewWork) + ','
						lmsg += 'ACTION_WorkType=' + str(self.constant.ACTION_WorkType)
						spark().sent_log(lmsg) 

						# ******* LOG END ******* 

						# Assign new stock to read
						jsonreply = '{'
						jsonreply += '"API_KEY":"' + self.constant.validated + '",'
						jsonreply += '"ACTION":"' + self.constant.ACTION_AssignNewWork + '",'
						jsonreply += '"WorkType":"' + self.constant.ACTION_WorkType + '",'
						jsonreply += '"stock":"' + stock + '",'
						jsonreply += '"maxlogdt":"' + maxlogdt + '"'
						jsonreply += '}' 
						#print(jsonreply)
						return jsonreply

			except Exception as e:
				return self.constant.action_error_return_code + ':' + str(e) # 

	def major_holders(self):

		retval = self.constant.error_return_code
		json_hr = json_handler()
		try:
			if 'Errormsg' in request.args:
				Errormsg = request.args.get('Errormsg')

			if 'stock' in request.args:
				stock = request.args.get('stock')

			if Errormsg.lower().find('delisted') > -1:
				iSQL = "INSERT INTO delisted(stock,error) VALUES ('" + stock + "','" + Errormsg.replace("'",'') + "')"
				spark().sql(iSQL)
			else:

				json_hr.load_request_json(request.json)
				#json_hr.show_cols()
				#major_holders
				iSQL = "INSERT INTO major_holders VALUES (CURRENT_TIMESTAMP,'" + stock + "',"
				iSQL += str(json_hr.data[0]['0']).replace('%','') + ','	# '% of Shares Held by All Insider'
				iSQL += str(json_hr.data[1]['0']).replace('%','') + ','	# '% of Shares Held by Institutions'
				iSQL += str(json_hr.data[2]['0']).replace('%','') + ','	# '% of Float Held by Institutions'
				iSQL += str(json_hr.data[3]['0']).replace('%','') + ')'	# 'Number of Institutions Holding Shares'
				spark().sql(iSQL)
				retval = self.constant.success_return_code

		except Exception as e:
			retval = self.constant.error_return_code + ': ' +  str(e) 

		return retval

	def stockinfo(self):

		retval = self.constant.error_return_code
		json_hr = json_handler()
		try:
			if 'Errormsg' in request.args:
				Errormsg = request.args.get('Errormsg')

			if 'stock' in request.args and 'rtype' in request.args:
				stock = request.args.get('stock') # testtest
				rtype = request.args.get('rtype') # dict

			if Errormsg.lower().find('delisted') > -1:
				iSQL = "INSERT INTO delisted(stock,error) VALUES ('" + stock + "','" + Errormsg.replace("'",'') + "')"
				spark().sql(iSQL)
			else:
				json_hr.load_dict(request.json)
				json_hr.onerow_insert_fromdict(stock,'info_on_stocks')
				retval = self.constant.success_return_code
		except Exception as e:
			retval = self.constant.error_return_code + ': ' +  str(e) 

		return retval

	def institutional_holders(self):

		retval = self.constant.error_return_code
		json_hr = json_handler()
		try:
			if 'Errormsg' in request.args:
				Errormsg = request.args.get('Errormsg')

			if 'stock' in request.args:
				stock = request.args.get('stock')

			if Errormsg.lower().find('delisted') > -1:
				iSQL = "INSERT INTO delisted(stock,error) VALUES ('" + stock + "','" + Errormsg.replace("'",'') + "')"
				spark().sql(iSQL)
			else:
				json_hr.load_request_json(request.json)

				json_hr.insert_to_table(stock,'institutional_holders')
				retval = self.constant.success_return_code

		except Exception as e:
			retval = self.constant.error_return_code + ': ' +  str(e) 

		return retval

	def recommendations(self):

		retval = self.constant.error_return_code
		json_hr = json_handler()
		Errormsg = ''
		stock = ''
		try:
			if 'Errormsg' in request.args:
				Errormsg = request.args.get('Errormsg')
			if 'stock' in request.args:
				stock = request.args.get('stock')
			
			if Errormsg.lower().find('delisted') > -1:
				iSQL = "INSERT INTO delisted(stock,error) VALUES ('" + stock + "','" + Errormsg.replace("'",'') + "')"
				spark().sql(iSQL)

			else:
				json_hr.load_request_json(request.json)
				json_hr.insert_to_table(stock,'recommendations')
				retval = self.constant.success_return_code

		except Exception as e:
			retval = self.constant.error_return_code + ': ' +  str(e) 

		return retval

	def calendar(self):

		retval = self.constant.error_return_code
		try:
			if 'Errormsg' in request.args:
				Errormsg = request.args.get('Errormsg')

			if 'stock' in request.args:
				stock = request.args.get('stock')

			if Errormsg.lower().find('delisted') > -1:
				iSQL = "INSERT INTO delisted(stock,error) VALUES ('" + stock + "','" + Errormsg.replace("'",'') + "')"
				spark().sql(iSQL)
			else:

				j = json.loads(request.json)
				x = str(j['data'][0]['index']) + ' ' + str(j['data'][0]['0']) + '\n'
				x += str(j['data'][1]['index']) + ' ' + str(j['data'][1]['0']) + '\n'
				x += str(j['data'][2]['index']) + ' ' + str(j['data'][2]['0']) + '\n'
				x += str(j['data'][3]['index']) + ' ' + str(j['data'][3]['0']) + '\n'
				x += str(j['data'][4]['index']) + ' ' + str(j['data'][4]['0']) + '\n'
				x += str(j['data'][5]['index']) + ' ' + str(j['data'][5]['0']) + '\n'
				x += str(j['data'][6]['index']) + ' ' + str(j['data'][6]['0']) + '\n'

				iSQL = "" 
				keeprunning = True
				i = 0
				while keeprunning:
					try:
						foundvalue = str(j['data'][0][str(i)])
						iSQL = "INSERT INTO earningsdate VALUES (CURRENT_TIMESTAMP,'" + stock + "',left('" + foundvalue + "',10));\n"
						i += 1
						spark().sql(iSQL)
					except:					
						keeprunning = False
					
				return self.constant.success_return_code

		except Exception as e:
			return self.constant.error_return_code + ': ' +  str(e) 


class json_handler:
	def __init__(self):
		self.constantolcount = 0
		self.data = None

	def load_request_json(self,rjson):
		self.json_str = json.loads(rjson)
		self.constantolumns = self.json_str['schema']['fields']
		self.constantolcount = len(self.constantolumns)
		self.data = self.json_str['data']
		self.rowcount = len(self.data)

	def show_cols(self):
		for i in range(0,self.constantolcount):
			if self.constantolumns[i]['type'] == 'datetime':				
				print('is datetime... ' + str(self.constantolumns[i]))
			elif self.constantolumns[i]['type'] == 'string':
				print('is string... ' + str(self.constantolumns[i]))
			else:
				print('is number... ' + str(self.constantolumns[i]))

	def insert_to_table(self,stock,tablename):
		retval = ''
		for i in range(0,self.rowcount):
			iSQL = "INSERT INTO " + tablename + " VALUES (CURRENT_TIMESTAMP,'" + stock + "',"
			for j in range(0,self.constantolcount):
				fldname = self.constantolumns[j]['name']
				fldtype = self.constantolumns[j]['type']
				if fldtype == 'datetime':				
					iSQL += "'" + str(self.data[i][fldname])[:10] + "'," 
				elif fldtype == 'string':
					iSQL += "'" + str(self.data[i][fldname]).replace("'",'') + "',"  
				else:
					iSQL += str(self.data[i][fldname]) + ","  

			iSQL = iSQL[:-1] + ");\n"
			try:
				spark().sql(iSQL)
			except Exception as e:
				retval += ':' + str(e)
		return retval

	def insert_to_stockdata(self,stock):
		retval = ''
		# 'stkdta'
		fldlist = {}
		fldlist['Date'] = 'log_dt'
		fldlist['Open'] = 'Open'
		fldlist['High'] = 'High'
		fldlist['Low'] = 'Low'
		fldlist['Close'] = 'Close'
		fldlist['Volume'] = 'Volume'

		for i in range(0,self.rowcount):
			iSQL = "INSERT INTO stockdata (stock,"

			for j in range(0,self.constantolcount):
				fldname = self.constantolumns[j]['name']
				if fldname in fldlist:
					iSQL += str(fldlist[fldname]).replace(' ','') + ','

			iSQL = iSQL[:-1] + ",update_dtm) VALUES ('" + stock + "'," 			
			
			for j in range(0,self.constantolcount):
				fldname = self.constantolumns[j]['name']
				fldtype = self.constantolumns[j]['type']
				if fldname in fldlist:
					if fldtype == 'datetime':				
						iSQL += "'" + str(self.data[i][fldname])[:10] + "'," 
					elif fldtype == 'string':
						iSQL += "'" + str(self.data[i][fldname]).replace("'",'') + "',"  
					else:
						iSQL += str(self.data[i][fldname]) + ","  

			iSQL = iSQL[:-1] + ",CURRENT_TIMESTAMP);\n"
			try:
				spark().sql(iSQL)
			except Exception as e:
				retval += ':' + str(e)

		return retval

	def load_dict(self,req_text):
		self.dict_str = json.loads(req_text)
		
	def onerow_insert_fromdict(self,stock,tablename):
		retval = ''
		iSQL = 'INSERT INTO ' + tablename + '(dtm,stock,'
		for field in self.dict_str:
			iSQL += field.replace(' ','') + ','
		iSQL = iSQL[:-1] + ") VALUES (CURRENT_TIMESTAMP,'" + stock + "'," 
		for field in self.dict_str:
			fldval = str(self.dict_str[field]).replace("'",'')
			if len(fldval) > 100:
				fldval = fldval[:100]
			
			iSQL += "'" + fldval + "',"
		iSQL = iSQL[:-1] + ")"
		try:
			spark().sql(iSQL)
		except Exception as e:
			retval += ':' + str(e)

		return retval 



# ***********************************************************************************
# sentinel - End
# ***********************************************************************************


