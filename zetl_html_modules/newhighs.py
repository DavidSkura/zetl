"""
  Dave Skura, 2021
"""
from flask import Flask, render_template,request,make_response,jsonify, url_for, session
from zetl_modules.zetl_utilities import db,cookie
from zetl_html_modules.admin import useradmin


class newhighs177:
	def __init__(self):
		self.sqldb = db()

	def newhighs177page(self):
		comment = """
			143,754 new highs (177 day)-- avg daily trade volume for 2021 is > 100k
			103,846 occurrences where new highs are within 5 days of a previous new high (buy_chance count)

			75,819 cases had at least 1 new 177 day high within 5 days of the occurrence	(0.73%)
			85,441 cases had at least 1 new 177 day high within 10 days of the occurrence	(0.82%)
			89,632 cases had at least 1 new 177 day high within 15 days of the occurrence	(0.86%)
			91,439 cases had at least 1 new 177 day high within 20 days of the occurrence	(0.89%)

			* this means there were 2 New highs within 5 days.  New High is based on previous max(high) over 177 days.
			* From analysis on data between 2019-04-01 and 2021-03-01
			* LoseChance number is a percent of how many times like items had no new highs after 10 days.  Low number here is best.
		"""

		username, allowed, redirect_bad = useradmin().call_url_allowed(request.cookies.get('username'))
		if not allowed:
			return "<HTML><BODY onload=document.location='" + redirect_bad + "'></BODY></HTML>"

		stock=''
		log_dt=''
		firm=''
		if 'stock' in request.args:
			stock = request.args.get('stock')
		if 'log_dt' in request.args:
			log_dt = request.args.get('log_dt')

		result_html = render_template('header.html', title=' Home ') + render_template('menu.html',username=username)

		isql = """
			SELECT * 
			FROM (
				SELECT *
				FROM vNewHighs177
				WHERE true  

		"""

		result_html += "<FORM method=GET action='newhighs177'>"
		result_html += "<INPUT type=button name=download value=download onclick=javascript:window.open('http://bettergardening.ngrok.io/static/newhigh177.csv');>   &nbsp;  &nbsp; "
		result_html += "Stock <INPUT type=text id=stock width=10 name='stock' value='" + stock + "'> "
		result_html += "log_dt <INPUT type=text id=log_dt width=10 name='log_dt' value='" + log_dt + "'> "
		result_html += "<INPUT type=submit name='submit' value='submit'> &nbsp; "

		self.sqldb = db()
		self.sqldb.dbconnect()
		self.sqldb.query("SELECT DISTINCT recent_newhigh_dt FROM (" + isql + ") L ) dist_qry ORDER BY recent_newhigh_dt desc limit 7")
		if self.sqldb.data:
			for row in self.sqldb.data:
				result_html += "<a href='newhighs177?log_dt=" + str(row[0]) + "'>" + str(row[0]) + "</a>, &nbsp "
		
		result_html += "</FORM>"
		result_html += """
	* There have been 2 New highs within 5 days.  New High is based on previous max(high) over 177 days.<BR>
	 - <strong>prv_newhigh_dt</strong> is trade date of previous new high.  <strong>prv_newhigh_amt</strong> is the amount of previous new high <BR>
	 - <strong>recent_newhigh_dt</strong> is trade date of most recent new high in this list.  <strong>recent_newhigh_amt</strong> is trade amount of most recent new high in this list<BR>
	 - <strong>recent_low</strong> is the low of the most recent new high<BR>
	 - <strong>increase_as_pct</strong> is the % diff in new high<BR>
	 - <strong>no_new_high</strong> number is a percent of how many times like items had no new highs after 10 days.  Low number here is best.<BR>
	 - <strong>lowpct_0</strong> average lowest low by pct in 10 days when no new high hit<BR>
	 - <strong>lowpct_gt0</strong> average lowest low by pct in 10 days when at least one new high hit.  <BR>
	 - <strong>lowpct_0_amt</strong> recent_low - (lowpct_0*recent_low)<BR>
	 - <strong>lowpct_gt0_amt</strong> recent_low - (lowpct_gt0*recent_low)<BR>
	 - <strong>LowAfterHigh_gt0pct</strong> percent of events where the 10 day Low was after the 10 day high<BR>
	 - <strong>HighAfterLow_gt0pct</strong> percent of events where the 10 day High was after the 10 day Low<BR>
		"""

		if len(stock) > 0:
			isql += " AND stock='"  + stock + "'"
		if len(log_dt) > 0:
			isql += " AND recent_newhigh_dt='"  + log_dt + "'"
		isql += " limit 250 ) L  "

		isql += cookie(None).getorders() 
		self.sqldb.RowsAffected = self.sqldb.cur.execute(isql)


		self.sqldb.linkcols.append('stock')
		self.sqldb.linkcolidxs.append(0)
		self.sqldb.linkcols.append('log_dt')
		self.sqldb.linkcolidxs.append(3)
		self.sqldb.linkonfield = 0	
		self.sqldb.linkpage='modelling'

		result_html += self.sqldb.tableout(self.sqldb.cur,True)

		result_html += render_template('footer.html') 
		
		self.sqldb.csvout('newhigh177.csv')

		result_html += render_template('footer.html') 
		self.sqldb.dbdone()

		return result_html
