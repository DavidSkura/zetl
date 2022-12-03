"""
  Dave Skura, 2021
"""
from flask import Flask, render_template,request,make_response,jsonify, url_for, session
from zetl_modules.zetl_utilities import db
from zetl_html_modules.admin import useradmin


class home:
	def __init__(self):
		self.sqldb = db()

	def homepage(self):
		username, allowed, redirect_bad = useradmin().call_url_allowed(request.cookies.get('username'))
		if not allowed:
			return "<HTML><BODY onload=document.location='" + redirect_bad + "'></BODY></HTML>"

		result_html = render_template('header.html', title=' Home ') + render_template('menu.html',username=username)

		self.sqldb.dbconnect()
		asql = "SELECT * FROM activity" # currently,previously,dtm,keyfld,prvkeyfld 
		self.sqldb.RowsAffected = self.sqldb.cur.execute(asql)
		result_html += self.sqldb.tableout(self.sqldb.cur,False,False,False)

		result_html += '<HR>'


		zsql = """
			SELECT datname, usename, client_addr, age(clock_timestamp(), query_start), concat(LEFT(query,100),'...') as sql
			FROM pg_stat_activity 
			WHERE query != '<IDLE>' AND query NOT ILIKE '%pg_stat_activity%' and query <> ''
			ORDER BY query_start desc
		"""
		self.sqldb.RowsAffected = self.sqldb.cur.execute(zsql)
		result_html += self.sqldb.tableout(self.sqldb.cur,False,False,False)

		result_html += '<HR>'

		#sql =  " SELECT dtm,log "
		#sql += " FROM sentinel_log" 
		#sql += " ORDER BY dtm desc limit 10"
		#self.sqldb.cur.execute(sql)
		#result_html += self.sqldb.tableout(self.sqldb.cur,False,False,False)


		result_html += render_template('footer.html') 

		self.sqldb.dbdone()

		return result_html
