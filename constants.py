"""
  Dave Skura, 2021
  
  File Description:
"""
import socket 

class dbinfo:
	def __init__(self):
		self.version=1.1
		self.updated='Apr 20/21'

		# ***** edit for flask config
		self.host_name = socket.gethostname() 
		self.host_ip = socket.gethostbyname(self.host_name) # for running http srvr
		self.host_ip = '127.0.0.1'							# needed for ngrok
		self.host_redir_ip = '108.162.188.237'  # random bad ip for redirect on error
		self.host_port = 5150

		# ***** edit this path for the installation to work *****
		self.dir_zetl_install = 'D:\\zetl\\'

		# static dir for file downloading
		self.dir_website_static = self.dir_zetl_install + 'static\\'

		# ***** edit these DB credentials for the installation to work *****
		self.idb='PostgreSQL 13.2'
		self.ihost='127.0.0.1'
		self.iport="5432"
		self.ischema='stock_market'
		self.iuser='dad'
		self.ipwd='dad'

		self.idescription='Upstairs desktop, running Postgres 13.2 in Development mode.'
		self.dir_data = self.dir_zetl_install + 'data'
		self.dir_install_ddl = self.dir_zetl_install + 'install_ddl'
		self.dir_zetl_modules = self.dir_zetl_install + 'zetl_modules'
		self.dir_zetl_scripts = self.dir_zetl_install + 'zetl_scripts'

		self.connection_str = self.idb + ', @' + self.ihost + ':' + self.iport + ', database=' + self.ischema + ', using ' + self.iuser + '/' + self.ipwd + '\n'

class sentinel_constants:
	def __init__(self):
		self.version = 2.0
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
		self.ACTION_WorkType = self.ACTION_WorkType_Wait
		self.CheckaStock = False
		self.validated="validated"
		self.success_return_code='E1072'
		self.error_return_code='E1172'	 
		self.action_error_return_code = 'E3453'

	def init(self,IncludeRecommend,IncludeData,Includetherest):
		if not IncludeRecommend and IncludeData and not Includetherest:
			self.ACTION_WorkType = self.ACTION_WorkType_DataOnly
			self.CheckaStock = True
		elif IncludeRecommend and IncludeData and Includetherest:
			self.ACTION_WorkType = self.ACTION_WorkType_PullAll
			self.CheckaStock = True
		elif IncludeRecommend and IncludeData and not Includetherest:
			self.ACTION_WorkType = self.ACTION_WorkType_RecAndDataOnly
			self.CheckaStock = True
		elif IncludeRecommend and not IncludeData and not Includetherest:
			self.ACTION_WorkType = self.ACTION_WorkType_RecOnly
			self.CheckaStock = True
		elif IncludeRecommend and not IncludeData and Includetherest:
			self.ACTION_WorkType = self.ACTION_WorkType_RecAndOther
			self.CheckaStock = True
		elif not IncludeRecommend and not IncludeData and Includetherest:
			self.ACTION_WorkType = self.ACTION_WorkType_TheRest
			self.CheckaStock = True
		else:
			self.ACTION_WorkType = self.ACTION_WorkType_Wait
			self.CheckaStock = False
