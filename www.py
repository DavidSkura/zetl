import psycopg2
import socket 

from flask import Flask, render_template,request,make_response,jsonify, url_for, session
from flask_googlecharts import GoogleCharts,MaterialLineChart


from zetl_modules.DaveChart import charter
from zetl_modules.constants import dbinfo
from zetl_modules.zetl_utilities import db

from zetl_html_modules.admin import useradmin
from zetl_html_modules.home import home
from zetl_html_modules.etl import etl
from zetl_html_modules.newhighs import newhighs177
from zetl_html_modules.buyandsell import buyandsell
from zetl_html_modules.modelling import modelling
from zetl_html_modules.sentinel_server import sentinel_server


import datetime

# ***********************************************************************************
# Global vars
# ***********************************************************************************

charts = GoogleCharts()

app = Flask(__name__)
app.secret_key = "def"  
charts.init_app(app)

sqldb = db()

# ***********************************************************************************
# www - Admin.  Site Admin.  User List.  Cookie Security.
# ***********************************************************************************

@app.route('/doAdminUserAction', methods=['POST'])
def doAdminUserAction_link():

	if 'doDeleteUser' in request.form:
		return useradmin().doDeleteUser()
	elif 'doCreateUser' in request.form:
		return useradmin().doCreateUser()

	return ''

@app.route('/dologin', methods=['POST'])
def dologin_link():
	return useradmin().dologin()

@app.route('/login')
def login_link():
	return useradmin().login()

@app.route('/admin')
def admin_link():
	return useradmin().admin()

@app.route('/')
@app.route('/home')
def home_link():
	return home().homepage()

# ***********************************************************************************
# etl - zetl jobs
# ***********************************************************************************

@app.route('/etl')
def etl_link():
	return etl().etlpage()

@app.route('/etl_run')
def etl_run_link():
	return etl().etl_run()

# ***********************************************************************************
# New Highs over 177 days
# ***********************************************************************************

@app.route('/newhighs177')
def newhighs177_link():
	return newhighs177().newhighs177page()

# ***********************************************************************************
# Planning & Tracking, Buying and Selling 
# ***********************************************************************************

@app.route('/buyandsell')
def buyandsell_link():
	return buyandsell().buyandsellpage()

# ***********************************************************************************
# Modelling daily activity
# ***********************************************************************************

@app.route('/modelling')
def modelling_link():
	username, allowed, redirect_bad = useradmin().call_url_allowed(request.cookies.get('username'))
	if not allowed:
		return "<HTML><BODY onload=document.location='" + redirect_bad + "'></BODY></HTML>"
	more_html,csql,fit_label = modelling().modellingpage()
	if csql!='':
		ctitle='fit_till: ' + fit_label
		cwidth=1000
		cheight=400
		charts.register(charter().lineit(csql,ctitle,cwidth,cheight))

	result_html = render_template('header.html', title='Modelling') + render_template('menu.html',username=username)
	result_html += more_html

	if csql!='':
		result_html += render_template("chartthis.html")

	result_html += render_template('footer.html') 

	return result_html



# ***********************************************************************************
# Sentinel Server
# ***********************************************************************************

@app.route('/stkdta', methods=['POST','GET'])
def stkdta_link():
	return sentinel_server().stkdta()

@app.route('/action', methods=['POST','GET'])
def action_link():
	return sentinel_server().action()

@app.route('/major_holders', methods=['POST','GET'])
def major_holders_link():
	return sentinel_server().major_holders()

@app.route('/stockinfo', methods=['POST','GET'])
def stockinfo_link():
	return sentinel_server().stockinfo()

@app.route('/institutional_holders', methods=['POST','GET'])
def institutional_holders_link():
	return sentinel_server().institutional_holders()

@app.route('/recommendations', methods=['POST','GET'])
def recommendations_link():
	return sentinel_server().recommendations()

@app.route('/calendar', methods=['POST','GET'])
def calendar_link():
	return sentinel_server().calendar()


# ***********************************************************************************
# www - Run the Server
# ***********************************************************************************

if __name__ == '__main__':
	app.run(debug=True,host=dbinfo().host_ip,port=dbinfo().host_port)
