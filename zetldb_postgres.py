"""
  Dave Skura, 2022
"""
import os
import sys
from postgresdave_package.postgresdave import db #install pip install postgresdave-package

class zetldb:

	def __init__(self):
		self.db = db()
		self.db.connect()

		self.version=2.0

	def export_zetl(self):
		
		etl_list = self.db.query('SELECT DISTINCT etl_name FROM ' + self.db.db_conn_dets.DB_SCHEMA + '.z_etl ORDER BY etl_name')
		for etl in etl_list:
			etl_name = etl[0]
			qry = "SELECT stepnum,sqlfile,steptablename,estrowcount FROM " + self.db.db_conn_dets.DB_SCHEMA + ".z_etl WHERE etl_name = '" + etl_name + "' ORDER BY stepnum"
			csv_filename = 'zetl_scripts\\' + etl_name + '\\z_etl.csv'
			self.db.export_query_to_csv(qry,csv_filename)

	def empty_zetl(self):
			dsql = "DELETE FROM " + self.db.db_conn_dets.DB_SCHEMA + ".z_etl "
			self.db.execute(dsql)
			self.db.commit()

	def load_z_etlcsv_if_forced(self,etl_name='',option=''):
		szdelimiter = ','
		if (etl_name != '' and option == '-f'):
			self.empty_zetl()
			qualified_table = self.db.db_conn_dets.DB_SCHEMA + ".z_etl"
			csv_filename = 'zetl_scripts\\' + etl_name + '\\z_etl.csv'
			f = open(csv_filename,'r')
			hdrs = f.read(1000).split('\n')[0].strip().split(szdelimiter)
			f.close()		
			
			isqlhdr = 'INSERT INTO ' + qualified_table + '('

			for i in range(0,len(hdrs)):
				isqlhdr += hdrs[i] + ','
			isqlhdr = isqlhdr[:-1] + ') VALUES '

			skiprow1 = 0
			ilines = ''

			with open(csv_filename) as myfile:
				for line in myfile:
					if skiprow1 == 0:
						skiprow1 = 1
					else:
						row = line.rstrip("\n").split(szdelimiter)

						newline = '('
						for j in range(0,len(row)):
							if row[j].lower() == 'none' or row[j].lower() == 'null':
								newline += "NULL,"
							else:
								newline += "'" + row[j].replace(',','').replace("'",'') + "',"
							
						ilines += newline[:-1] + ')'
						
						qry = isqlhdr + ilines
						ilines = ''
						self.db.execute(qry)
						self.db.commit()

	def is_an_int(self,prm):
			try:
				if int(prm) == int(prm):
					return True
				else:
					return False
			except:
					return False

	def add_etl_step(self,p_etl_name,p_etl_step,p_etl_filename):
		isql = "INSERT INTO " + self.db.db_conn_dets.DB_SCHEMA + ".z_etl(etl_name,stepnum,sqlfile) VALUES ('" + p_etl_name + "'," + p_etl_step + ",'" + p_etl_filename + "')"
		self.db.execute(isql)
		self.db.commit()
		#print('Adding ' + p_etl_name + '\\' + p_etl_filename)

	def etl_step_exists(self,etl_name,etl_step):
		sql = "SELECT COUNT(*) FROM " + self.db.db_conn_dets.DB_SCHEMA + ".z_etl WHERE etl_name = '" + etl_name + "' and stepnum = " + etl_step
		etlrowcount = self.db.queryone(sql)
		if etlrowcount == 0:
			return False
		else:
			return True

	def load_folders_to_zetl(self,this_etl_name='all'):
		etl_folder = 'zetl_scripts'
		subdirs = [x[0] for x in os.walk(etl_folder)]
		for i in range(0,len(subdirs)):
			possible_etl_dir = subdirs[i]
			if len(possible_etl_dir.split('\\')) == 2:
				etl_name = possible_etl_dir.split('\\')[1]
				if (this_etl_name == 'all' or etl_name == this_etl_name):
					
					dir_list = os.listdir(etl_folder + '\\' + etl_name)
					for etl_script_file in os.listdir(etl_folder + '\\' + etl_name):
						if etl_script_file.endswith(".sql") or etl_script_file.endswith(".ddl") or etl_script_file.endswith(".py") or etl_script_file.endswith(".bat"):
							if len(etl_script_file.split('.')) == 3:
								etl_step = etl_script_file.split('.')[0]
								file_suffix = etl_script_file.split('.')[1] + '.' + etl_script_file.split('.')[2]
								if self.is_an_int(etl_step):
									if not self.etl_step_exists(etl_name,etl_step):
										self.add_etl_step(etl_name,etl_step,etl_script_file)		


if __name__ == '__main__':
	print ("db command line test") # 
	print('')
	myzetldb = zetldb()
	
