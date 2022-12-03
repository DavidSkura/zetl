SENTINEL_ID = 'python anywhere (daveskura@hotmail.com)'
SENTINEL_CLIENT_VERSION = 2.7 # ver 1.0 is in sentinel_source/sentinel/

# importing the requests library 
import time
import requests
import sys
import json
import yfinance as yf
import lxml
import pandas as pd

IncludeRecommend = True
IncludeData = True
Includetherest = True

samplecall="""
from client import worker
x = worker().run(15)
"""

class myconstants:
	def __init__(self):
		self.urlprefix = 'http://bettergardening.ngrok.io'
		self.jctrl_action_url = self.urlprefix + "/action"
		self.jctrl_earnings_url = self.urlprefix + "/calendar"
		self.jctrl_recommendations_url = self.urlprefix + "/recommendations"
		self.jctrl_major_holders_url = self.urlprefix + "/major_holders"
		self.jctrl_iholders_url = self.urlprefix + "/institutional_holders"
		self.jctrl_stockinfo_url = self.urlprefix + "/stockinfo"
		self.jctrl_stkdta_url = self.urlprefix + "/stkdta"

		self.API_KEY = 'aefkjaskldjfhklahsdfuilsjkdxcvn m,.ncjhfu489wedu(&hIup9%#@&E^*RUOJHLGU^T(UGJHK'
		self.ACTION_NewWorkAction = "Request New Work Item"
		self.ACTION_AssignNewWork = "Read New Stock"
		self.ACTION_WorkType_DataOnly = "Data Only"
		self.ACTION_WorkType_RecOnly = "Recommendations Only"
		self.ACTION_WorkType_RecAndDataOnly = "Recommendations and Data"
		self.ACTION_WorkType_PullAll = "Recommendations and Data and the Rest"
		self.ACTION_WorkType_RecAndOther = "Recommendations, not Data, and the Rest"
		self.ACTION_WorkType_TheRest = "Not Recommendations, not Data, just the Rest"
		self.ACTION_WorkType_Wait = "Do Nothing, Just Wait"

		self.validated="validated"
		self.success_return_code='E1072'
		self.error_return_code='E1172'	 
 
class worker:
	def run30(self):
		self.run(30)

	def run(self,delayamount):
		print("started")
		Errormsg = ''
		looper= True
		jobsentinel = ''
		while looper:	
			Errormsg = ''
			response = requests.post(url = myconstants().jctrl_action_url, data={"API_KEY":myconstants().API_KEY,"ACTION":myconstants().ACTION_NewWorkAction,"SENTINEL_ID":SENTINEL_ID})
			try:
				jobsentinel = json.loads(response.text)
			except Exception as e:
				print(str(e))
				print(response.text)

			if jobsentinel["API_KEY"] == myconstants().validated and jobsentinel["ACTION"] == myconstants().ACTION_AssignNewWork:
				print('update ' + jobsentinel["stock"] + ' since ' +	jobsentinel["maxlogdt"] + ', ' + jobsentinel["WorkType"])
				inet_stockdata = yf.Ticker(jobsentinel["stock"])			 

				IncludeRecommend = False
				IncludeData = False
				Includetherest = False
				
				if jobsentinel["WorkType"] == myconstants().ACTION_WorkType_RecOnly:
					IncludeRecommend = True
					IncludeData = False
					Includetherest = False
				elif jobsentinel["WorkType"] == myconstants().ACTION_WorkType_DataOnly:
					IncludeRecommend = False
					IncludeData = True
					Includetherest = False
				elif jobsentinel["WorkType"] == myconstants().ACTION_WorkType_RecAndDataOnly:
					IncludeRecommend = True
					IncludeData = True
					Includetherest = False
				elif jobsentinel["WorkType"] == myconstants().ACTION_WorkType_PullAll:
					IncludeRecommend = True
					IncludeData = True
					Includetherest = True
				elif jobsentinel["WorkType"] == myconstants().ACTION_WorkType_RecAndOther:
					IncludeRecommend = True
					IncludeData = False
					Includetherest = True
				elif jobsentinel["WorkType"] == myconstants().ACTION_WorkType_TheRest:
					IncludeRecommend = False
					IncludeData = False
					Includetherest = True
				elif jobsentinel["WorkType"] == myconstants().ACTION_WorkType_Wait:
					time.sleep(60)
					IncludeRecommend = False
					IncludeData = False
					Includetherest = False

				if IncludeRecommend:
					data_recommend = None
					try:
						data_recommend = inet_stockdata.recommendations
					except Exception as e:
						Errormsg = str(e)
						data_recommend = None

					if isinstance(data_recommend, pd.DataFrame):
						if not data_recommend.empty:
							cURL = myconstants().jctrl_recommendations_url + '?stock=' + jobsentinel["stock"] + '&mxlogdt=' + jobsentinel["maxlogdt"] + '&Errormsg=' + Errormsg
							response = requests.post(url = cURL, json=data_recommend.to_json(orient="table"),headers={'Content-Type': 'application/json'}) 
							if response.text != 'E1072':
								print('show analysts recommendations: ' + response.text[:50])
					else:
						cURL = myconstants().jctrl_recommendations_url + '?stock=' + jobsentinel["stock"] + '&Errormsg=' + Errormsg
						response = requests.post(url = cURL, json='') 

				if IncludeData:
					stckdata = None
					try:
						stckdata = inet_stockdata.history(start=jobsentinel["maxlogdt"])
					except Exception as e:
						Errormsg = str(e)
						stckdata = None

					if isinstance(stckdata, pd.DataFrame):
						if not stckdata.empty:
							cURL = myconstants().jctrl_stkdta_url + '?stock=' + jobsentinel["stock"] + '&mxlogdt=' + jobsentinel["maxlogdt"] + '&Errormsg=' + Errormsg
							response = requests.post(url = cURL, json=stckdata.to_json(orient="table"),headers={'Content-Type': 'application/json'})
							if response.text != 'E1072':
								print('history: '	+ response.text[:50])
					else:
						cURL = myconstants().jctrl_stkdta_url + '?stock=' + jobsentinel["stock"] + '&Errormsg=' + Errormsg
						response = requests.post(url = cURL, json='')


				if Includetherest:
					data_stockinfo = None
					try:
						data_stockinfo = json.dumps(inet_stockdata.info)
					except Exception as e:
						Errormsg = str(e)
						data_stockinfo = None

					if isinstance(data_stockinfo, pd.DataFrame):
						if not data_stockinfo.empty:
							cURL = myconstants().jctrl_stockinfo_url + '?stock=' + jobsentinel["stock"] + '&rtype=dict&mxlogdt=' + jobsentinel["maxlogdt"] + '&Errormsg=' + Errormsg
							response = requests.post(url = cURL, json=data_stockinfo,headers={'Content-Type': 'application/json'})
							if response.text != 'E1072':
								print('stockinfo: ' + response.text[:50])
					else:
						cURL = myconstants().jctrl_stockinfo_url + '?stock=' + jobsentinel["stock"] + '&Errormsg=' + Errormsg
						response = requests.post(url = cURL, json='')

					
					data_calendar = None
					try:
						data_calendar = inet_stockdata.calendar
					except Exception as e:
						Errormsg = str(e)
						data_calendar = None

					if isinstance(data_calendar, pd.DataFrame):
						if not data_calendar.empty:
							data_calendar = inet_stockdata.calendar
							cURL = myconstants().jctrl_earnings_url + '?stock=' + jobsentinel["stock"] + '&mxlogdt=' + jobsentinel["maxlogdt"] + '&Errormsg=' + Errormsg
							response = requests.post(url = cURL, json=data_calendar.to_json(orient="table"),headers={'Content-Type': 'application/json'}) 
							if response.text != 'E1072':
								print('adds to earningsdate table: ' + response.text[:50])
					else:
						cURL = myconstants().jctrl_earnings_url + '?stock=' + jobsentinel["stock"] + '&Errormsg=' + Errormsg
						response = requests.post(url = cURL, json='')

			
					
					data_major_hldrs = None
					try:
						data_major_hldrs = inet_stockdata.major_holders
					except Exception as e:
						Errormsg = str(e)
						data_major_hldrs = None
							
					if isinstance(data_major_hldrs, pd.DataFrame):
						if not data_major_hldrs.empty:
							cURL = myconstants().jctrl_major_holders_url + '?stock=' + jobsentinel["stock"] + '&mxlogdt=' + jobsentinel["maxlogdt"] + '&Errormsg=' + Errormsg
							response = requests.post(url = cURL, json=data_major_hldrs.to_json(orient="table"),headers={'Content-Type': 'application/json'}) 
							if response.text != 'E1072':
								print('major_holders: ' + response.text[:50])
					else:
						cURL = myconstants().jctrl_major_holders_url + '?stock=' + jobsentinel["stock"] + '&Errormsg=' + Errormsg
						response = requests.post(url = cURL, json='')

					data_iholders = None
					try:
						data_iholders = inet_stockdata.institutional_holders
					except Exception as e:
						Errormsg = str(e)
						data_iholders = None

					if isinstance(data_iholders, pd.DataFrame):
						if not data_iholders.empty:
							cURL = myconstants().jctrl_iholders_url + '?stock=' + jobsentinel["stock"] + '&mxlogdt=' + jobsentinel["maxlogdt"] + '&Errormsg=' + Errormsg
							response = requests.post(url = cURL, json=data_iholders.to_json(orient="table"),headers={'Content-Type': 'application/json'})
							if response.text != 'E1072':
								print('show institutional holders: ' + response.text[:50])
					else:
						cURL = myconstants().jctrl_iholders_url + '?stock=' + jobsentinel["stock"] + '&Errormsg=' + Errormsg
						response = requests.post(url = cURL, json='')




			else:
				print('no work assigned')
				sys.exit(0)
			time.sleep(delayamount)

x = worker().run(1)