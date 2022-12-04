"""
  Dave Skura, Nov 10,2019

"""
import sys
from zetl_modules.zetl_utilities import db
from flask_googlecharts import MaterialLineChart,BarChart


class charter:
	def lineit(self,csql,ctitle,cwidth,cheight):

		animation_option={ "startup" : True, "duration": 3000, "easing":'out'}

		lineit_chart = MaterialLineChart("chartiemccharterson", options={"title": ctitle,
																									"width": cwidth,
																									"height": cheight,
																									"animation": animation_option,
																									"slantedText": True}
																									)

		sqldb = db()
		sqldb.dbconnect()
		sqldb.RowsAffected = sqldb.cur.execute(csql)
		cols = sqldb.getcolumns(sqldb.cur)
		data = sqldb.cur.fetchall()

		for i in range(0,len(cols)):
			if cols[i].find('_dt') > -1:
				lineit_chart.add_column("string", cols[i])
			elif cols[i].lower() == 'day':
				lineit_chart.add_column("string", cols[i])
			else:
				lineit_chart.add_column("number", cols[i])

		dataset = []
		for row in data:
			r = []
			for i in range(0,len(cols)):
				if row[i] != None:
					c = str(row[i])
					if str(c).find(',') > -1:
						r.append(c.replace(',',''))
					else:
						r.append(c)
				else:
					r.append(None)

			dataset.append(r)
		
		sqldb.dbdone()

		lineit_chart.add_rows(dataset)

		return lineit_chart

