"""
  Dave Skura, 2022
"""
import socket
class variables:
	def __init__(self):
		self.version=1.0
		self.updated='Aug 29/22'
		self.database_hostname = 'localhost'
		self.database_ip_address = socket.gethostbyname(self.database_hostname)
		# ***** edit this path for the installation to work *****
		self.dir_zetl_install = 'D:\\zetl\\'

		# static dir for file downloading
		self.dir_website_static = self.dir_zetl_install + 'static\\'

		# ***** edit these DB credentials for the installation to work *****
		self.idb='Postgres'
		self.ihost= self.database_hostname # self.database_ip_address # '192.168.0.110' #'megapc' 
		self.iport="5432"
		self.ischema='nfl'
		self.iuser='dad'
		self.ipwd='dad'

		self.idescription='Upstairs desktop, running MySQL.'
		self.dir_data = self.dir_zetl_install + 'data'
		self.dir_install_ddl = self.dir_zetl_install + 'install_ddl'
		self.dir_zetl_modules = self.dir_zetl_install + 'zetl_modules'
		self.dir_zetl_scripts = self.dir_zetl_install + 'zetl_scripts'

		self.connection_str = self.idb + ', @' + self.ihost + ':' + self.iport + ', database=' + self.ischema + ', using ' + self.iuser + '/' + self.ipwd + '\n'

		self.showsql = True


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

