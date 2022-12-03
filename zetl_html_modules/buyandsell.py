"""
  Dave Skura, 2021
"""
from flask import Flask, render_template,request,make_response,jsonify, url_for, session
from zetl_modules.zetl_utilities import db
from zetl_html_modules.admin import useradmin

class buyandsell:
	def __init__(self):
		self.sqldb = db()

	def buyandsellpage(self):
		username, allowed, redirect_bad = useradmin().call_url_allowed(request.cookies.get('username'))
		if not allowed:
			return "<HTML><BODY onload=document.location='" + redirect_bad + "'></BODY></HTML>"
		result_html = render_template('header.html', title=' Home ') + render_template('menu.html',username=username)

		self.sqldb.dbconnect()

		stock = ''
		investment = '1000'
		count = ''
		buy_unit_count = 0
		buy_unit_price = ''
		doBuyit = ''
		doSellit = ''
		doDeleteit = ''
		if 'stock' in request.args:
			stock = request.args.get('stock')

		if 'investment' in request.args:
			investment = request.args.get('investment')

		if 'price' in request.args:
			buy_unit_price = request.args.get('price')
			sell_unit_price = buy_unit_price


		if 'count' in request.args:
			i = request.args.get('count')
			if i !='':
				count = int(i)
			else:
				count = 0
			sell_unit_count = count

		if buy_unit_price != '' and investment != '' and count == 0:
			buy_unit_count = int(float(investment)/float(buy_unit_price))
		else:
			buy_unit_count = count

		if 'doBuyit' in request.args:
			doBuyit = request.args.get('doBuyit')

		if 'doSellit' in request.args:
			doSellit = request.args.get('doSellit')

		if 'doDeleteit' in request.args:
			doDeleteit = request.args.get('doDeleteit')

		if stock != '' and buy_unit_price != '' and buy_unit_count > 0 and doBuyit != '':
			sql = "INSERT INTO tracker (stock,buy_units,buy_price) VALUES ('" + stock + "'," + str(buy_unit_count) + "," + buy_unit_price + ")"
			self.sqldb.execute(sql)
			self.sqldb.dbconn.commit()

		if stock != '' and buy_unit_price != '' and buy_unit_count > 0 and doSellit != '':
			sql = "UPDATE tracker SET sell_units = " + str(sell_unit_count)  + ", sell_price = " + sell_unit_price + " WHERE stock = '" + stock + "'"
			self.sqldb.execute(sql)
			self.sqldb.dbconn.commit()

		if stock != '' and buy_unit_price != '' and doDeleteit != '':
			sql = "DELETE FROM tracker WHERE stock = '" + stock + "' and buy_price = " + buy_unit_price
			self.sqldb.execute(sql)
			self.sqldb.dbconn.commit()

		
		result_html += """<FORM action="buyandsell" method = "GET" enctype = "multipart/form-data">"""
		result_html += """	Stock: <INPUT type=text name='stock'" value='""" + stock + """' size=5>  &nbsp|&nbsp 
												Unit Count #: <INPUT type=text name='count'" value='""" + str(buy_unit_count) + """' size=5>  &nbsp|&nbsp  
												Unit Price $: <INPUT type=text name='price'" value='""" + buy_unit_price + """' size=5>  &nbsp|&nbsp  
												Investment Cap$: <INPUT type=text name='investment'" value='""" + investment + """' size=5>  &nbsp|&nbsp  
												<INPUT type=submit name='doCalc' value='Calculate'>
												<INPUT type=submit name='doBuyit' value='Buy it'>
												<INPUT type=submit name='doSellit' value='Sell it'>
												<INPUT type=submit name='doDeleteit' value='Delete it'>
											</FORM>
									"""

		if buy_unit_price != '' and buy_unit_count > 0:
			result_html += 'Stock: ' + stock + '<BR>'
			result_html += ' Price: $' + str(buy_unit_price) + '<BR>'
			result_html += ' Units: ' + str(buy_unit_count) + '<BR>'
			result_html += ' Buy Cost: $' + str(round(buy_unit_count*float(buy_unit_price),2)) + '<BR>'

		result_html += '<HR>'
		sql = """	SELECT * 
									,round(buy_units*buy_price,2) as cost	
									,round(COALESCE(sell_units,0)*COALESCE(sell_price,0),2) as value
							FROM tracker 
							ORDER BY stock 
					"""
		self.sqldb.execute(sql)
		result_html += self.sqldb.tableout(self.sqldb.cur,False)

		result_html += render_template('footer.html') 
		self.sqldb.dbdone()
		return result_html

