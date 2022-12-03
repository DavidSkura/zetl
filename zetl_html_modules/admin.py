"""
  Dave Skura, 2021
"""
from flask import Flask, render_template,request,make_response,jsonify, url_for, session
from zetl_modules.zetl_utilities import db
from zetl_modules.constants import dbinfo

import datetime

class useradmin:
	def __init__(self):
		self.sqldb = db()

	def doDeleteUser(self):
		username = request.form['username']
		if not username :
			return "<HTML><BODY onload=document.location='/admin?error=no_username'></BODY></HTML>"

		else:
			self.sqldb.dbconnect()
			sql = "SELECT count(*) FROM webusers WHERE username='" + username + "'"
			ucount = self.sqldb.queryone(sql)
			if ucount > 0:
				msg = 'user deleted'
				sql = "DELETE FROM webusers WHERE username='" + username + "'"
				self.sqldb.execute(sql)
			else:
				msg = 'user not found'
			self.sqldb.dbdone()
			return "<HTML><BODY onload=document.location='/admin?" + msg.replace(' ','_') + "'></BODY></HTML>"


	def doCreateUser(self):
		username = request.form['username']
		password = request.form['password']
		ipaddress = request.form['ipaddress']
		if not username or not password:
			return "<HTML><BODY onload=document.location='/admin?error=no_username_or_password'></BODY></HTML>"

		else:
			self.sqldb.dbconnect()
			sql = "SELECT count(*) FROM webusers WHERE username='" + username + "' and password = '" + password + "'"
			ucount = self.sqldb.queryone(sql)
			if ucount > 0:
				#user already exists
				sql = "UPDATE webusers SET password = '" + password + "' WHERE username='" + username + "'"
				self.sqldb.execute(sql)
			else:
				#INSERT new user 
				sql = "INSERT INTO webusers (username,password,ipaddress) VALUES ('" + username + "','" + password + "','" + ipaddress + "')"
				self.sqldb.execute(sql)
			self.sqldb.dbdone()
			return "<HTML><BODY onload=document.location='/admin'></BODY></HTML>"

	def dologin(self):
		username = request.form['username']
		password = request.form['password']
		redirect_url = request.form['redirect_url']

		if not username or not password:
			return "<HTML><BODY onload=document.location='/login?username=" + username + "'></BODY></HTML>"
		
		else:
			self.sqldb.dbconnect()
			sql = "SELECT count(*) FROM webusers WHERE username='" + username + "' and password = '" + password + "'"
			ucount = self.sqldb.queryone(sql)
			if ucount == 0:
				return "<HTML><BODY onload=document.location='/login?username=" + username + "'></BODY></HTML>"
			else:
				# set cookie
				result_html = "<HTML><BODY onload=document.location='" + redirect_url + "'></BODY></HTML>"
				expire_date = datetime.datetime.now()
				expire_date = expire_date + datetime.timedelta(days=900)
				resp = make_response(result_html)
				resp.set_cookie('username', value=username,expires=expire_date)
				return resp

	def login(self):
		username = ''
		if 'username' in request.args:
			username = request.args.get('username')

		redirect_url = '/home'
		if 'redirect_url' in request.args:
			redirect_url = request.args.get('redirect_url')

		result_html = """
			<html>
					<head>
							<title>Login</title>
					</head>
					<body>
					<FORM action="dologin" method = "POST" enctype = "multipart/form-data">

						Username: <INPUT type=text name=username value='""" + username + """' size=10> <BR>
						Password: <INPUT type=text name=password value='' size=10> <BR>
						<INPUT type=hidden name=redirect_url value='""" + redirect_url + """' > 
											
						<INPUT type=submit value='Submit'>
					
					</FORM>
					</body>
			</html>
			"""
		return result_html

	def admin(self):
		username = ''
		username, allowed, redirect_bad = self.call_url_allowed(request.cookies.get('username'))
		gusername=''
		if 'username' in request.args:
			gusername = request.args.get('username')

		if gusername != 'dad' and not allowed:
			return "<HTML><BODY onload=document.location='" + redirect_bad + "'></BODY></HTML>"
		
		if username != '' or gusername == 'dad':
			result_html = render_template('header.html', title=' Home ') + render_template('menu.html',username=username)

			if gusername != '':
				result_html += "You are logged in as: " + gusername + "<BR>"
			elif username != '':
				result_html += "You are logged in as: " + username + "<BR>" 

			self.sqldb.dbconnect()
			self.sqldb.execute('SELECT * FROM webusers ORDER by dtm desc')
			result_html += self.sqldb.htmltableout(self.sqldb.cur)

			result_html += """<BR>
						<FORM action="doAdminUserAction" method = "POST" enctype = "multipart/form-data">

							Username: <INPUT type=text name=username value='' size=10> <BR>
							IPAddress: <INPUT type=text name=ipaddress value='' size=10> <BR>
							Password: <INPUT type=text name=password value='' size=10> <BR><BR>
												
							<INPUT type=submit name='doCreateUser' value='Create User'>
							<INPUT type=submit name='doDeleteUser' value='Delete User'>
						</FORM>
				"""		
			result_html += render_template('footer.html') 
			return result_html

		else:
			return "<HTML><BODY onload=document.location='" + redirect_bad + "'></BODY></HTML>"

	def call_url_allowed(self,cookie_username):
		if cookie_username:
			username = cookie_username
			allowed = True
			redirect_bad = ''
		else:
			username = 'unknown'
			allowed = False
			#redirect_bad = 'http://www.google.com/' + dbinfo().host_redir_ip
			redirect_bad = '/login'

		return username,allowed,redirect_bad
