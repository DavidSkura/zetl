"""
  Dave Skura, 2021
"""
import subprocess
from flask import Flask, render_template,request,make_response,jsonify, url_for, session
from zetl_modules.zetl_utilities import db
from zetl_html_modules.admin import useradmin


class etl:
	def __init__(self):
		self.sqldb = db()

	def etl_run(self):
		username, allowed, redirect_bad = useradmin().call_url_allowed(request.cookies.get('username'))
		if not allowed:
			return "<HTML><BODY onload=document.location='" + redirect_bad + "'></BODY></HTML>"

		etl_name = ''

		if 'etl_name' in request.args:
			etl_name = request.args.get('etl_name')

			shell_cmd = "python zetl.py " + etl_name + " -f"
			subprocess.Popen(shell_cmd, shell=True)

		if 'run_bat' in request.args:
			run_all_etl_jobs = request.args.get('run_bat')

			shell_cmd = run_all_etl_jobs + '.bat'
			subprocess.Popen(shell_cmd, shell=True)

		result_html =  "<HTML><BODY onload=document.location='/etl'></BODY></HTML>" 

		return result_html

	def etlpage(self):
		username, allowed, redirect_bad = useradmin().call_url_allowed(request.cookies.get('username'))
		if not allowed:
			return "<HTML><BODY onload=document.location='" + redirect_bad + "'></BODY></HTML>"

		etl_name = ''
		etl_dependencies = ''
		if 'etl_name' in request.args:
			etl_name = request.args.get('etl_name')

		if 'etl_dependencies' in request.args:
			etl_dependencies = request.args.get('etl_dependencies')

		result_html = render_template('header.html', title='ETL Jobs') + render_template('menu.html',username=username)

		self.sqldb = db()
		self.sqldb.dbconnect()

		zsql = """
			SELECT datname, usename, client_addr, age(clock_timestamp(), query_start), concat(LEFT(query,100),'...') as sql
			FROM pg_stat_activity 
			WHERE query != '<IDLE>' AND query NOT ILIKE '%pg_stat_activity%' and query <> ''
			ORDER BY query_start desc
		"""
		self.sqldb.RowsAffected = self.sqldb.cur.execute(zsql)
		result_html += self.sqldb.tableout(self.sqldb.cur,False,False,False)

		result_html += '<BR>' 

		zsql = """
			SELECT ETL.etl_name,A.max_rundtm,dependent_ETLs,CASE WHEN B.status is null THEN '' ELSE B.status END as status
				,coalesce(etlrunseconds::varchar,'n/a') as prev_etl_seconds
				,CASE WHEN B.status is not null THEN
						(A.max_rundtm + (coalesce(etlrunseconds,9999) ||' seconds')::interval)::varchar
				ELSE
						''
				END as etc
			FROM (SELECT DISTINCT etl_name FROM z_etl) ETL
					LEFT JOIN (
							SELECT etl_name, max(rundtm) as max_rundtm
							FROM z_log
							group by etl_name
							) A ON (ETL.etl_name = A.etl_name)
					LEFT JOIN ( SELECT etl_name,status 
											FROM etl_running 
											WHERE status='Running'
											) B ON (ETL.etl_name = B.etl_name)
					LEFT JOIN (
											SELECT etl_name, SUM(CASE WHEN etl_required <> 'raw' THEN 1 ELSE 0 END) as dependent_ETLs
											FROM z_etl_dependencies
											group by etl_name 
											) dep ON (A.etl_name = dep.etl_name)
					LEFT JOIN (
										SELECT A.etl_name
												,round(
															(
																(	
																		EXTRACT(HOURS FROM max(endtime))*60*60 
																	+ EXTRACT(MINUTES FROM max(endtime))*60 
																	+ EXTRACT(seconds FROM max(endtime) 
																)
															)::float
															- (
																		EXTRACT(HOURS FROM min(starttime))*60*60 
																	+	EXTRACT(MINUTES FROM min(starttime))*60 
																	+ EXTRACT(seconds FROM min(starttime))
																)::float
															)::numeric       
												,2) as etlrunseconds
										FROM z_log A
										INNER JOIN (
														SELECT etl_name,rundtm as previous_rundtm
														FROM (
																		SELECT etl_name,rundtm,RANK() OVER (PARTITION BY etl_name ORDER BY rundtm desc) rnk
																		FROM (SELECT DISTINCT etl_name,rundtm FROM z_log) subqry
																		) L
														WHERE rnk=2
														) prv ON (A.etl_name = prv.etl_name and A.rundtm = prv.previous_rundtm)
										GROUP BY A.etl_name
										) prv_runs ON (A.etl_name = prv_runs.etl_name)

			ORDER BY max_rundtm desc
		"""

		self.sqldb.cur.execute(zsql)
		data = self.sqldb.cur.fetchall()
		
		if len(data) > 0: 
			result_html += '<TABLE class=normal>' 
			result_html += '<TR class=normal>' 
			result_html += "<TD width=50 class=normal><STRONG>run_this_etl</STRONG></TD>" 
			result_html += "<TD width=50 class=normal><STRONG>max_rundtm</STRONG></TD>" 
			result_html += "<TD width=50 class=normal><STRONG>view etl</STRONG></TD>  "
			result_html += "<TD width=50 class=normal><STRONG>ETL Dependencies</STRONG></TD>" 
			result_html += "<TD width=50 class=normal><STRONG>status</STRONG></TD>" 
			result_html += "<TD width=50 class=normal><STRONG>prev_etl_seconds</STRONG></TD>" 
			result_html += "<TD width=50 class=normal><STRONG>etc</STRONG></TD>" 
			result_html += '</TR>' 

			for row in data:
				result_html += '<TR class=normal>' 
				result_html += "<TD class=normal> run <a href='etl_run?etl_name=" + str(row[0]) + "'>" + str(row[0]) + "</a> now.</TD>  "
				result_html += "<TD class=normal>" + str(row[1]) + "</TD>  "
				result_html += "<TD class=normal><a href='etl?etl_name=" + str(row[0]) + "'>" + str(row[0]) + "</a></TD>  "
				result_html += "<TD class=normal> " + str(row[2])  + "</TD>  "
				result_html += "<TD class=normal>" + str(row[3]) + "</TD>  "
				result_html += "<TD class=normal>" + str(row[4]) + "</TD>  "
				result_html += "<TD class=normal>" + str(row[5]) + "</TD>  "
				result_html += '</TR>' 

			result_html += '</TABLE><BR>' 


		if etl_name != '':
			zsql = """
					SELECT *
					FROM vETL_Branches
					WHERE root='""" + etl_name + """' or branch = '""" + etl_name + """' or twig = '""" + etl_name + """' 
					ORDER BY root

			"""
			self.sqldb.RowsAffected = self.sqldb.cur.execute(zsql)
			result_html += self.sqldb.tableout(self.sqldb.cur,False,False,False)

			result_html += '<BR>' 

			zsql = """
					SELECT *
					FROM z_etl_dependencies
					WHERE etl_name='""" + etl_name + """' 
					ORDER BY table_required

			"""
			self.sqldb.RowsAffected = self.sqldb.cur.execute(zsql)
			result_html += self.sqldb.tableout(self.sqldb.cur,False,False,False)

			result_html += '<BR>' 


			zsql = """
				SELECT 
					Z.rundtm,Z.etl_name,concat(Z.stepnum::varchar,'/',Z.part::varchar) as step,steptablename
						,COALESCE(rowcount,rows_affected) as rows
						,concat(LEFT(sql_to_run,50),'...') as sql
						,previous_runseconds as prv_runseconds
										,round(((EXTRACT(HOURS FROM endtime)*60*60 + EXTRACT(MINUTES FROM endtime)*60 + EXTRACT(seconds FROM endtime))::float
																- (EXTRACT(HOURS FROM starttime)*60*60 + EXTRACT(MINUTES FROM starttime)*60 + EXTRACT(seconds FROM starttime))::float)::numeric       
										,2) as runseconds
						,sql_error
						,replace(SUBSTR(sqlfile,POSITION('.' in sqlfile)-3),'\\','') as sqlfile
						,starttime
						,(starttime + (coalesce(previous_runseconds,9999) ||' seconds')::interval) as etc
						
						,endtime
						
				FROM z_log Z
				LEFT JOIN (
										
										SELECT A.etl_name,A.rundtm,A.stepnum,A.part
												,COALESCE(rowcount,rows_affected) as rows
												,round(((EXTRACT(HOURS FROM endtime)*60*60 + EXTRACT(MINUTES FROM endtime)*60 + EXTRACT(seconds FROM endtime))::float
																		- (EXTRACT(HOURS FROM starttime)*60*60 + EXTRACT(MINUTES FROM starttime)*60 + EXTRACT(seconds FROM starttime))::float)::numeric       
															,2) as previous_runseconds
										
										FROM z_log A
										INNER JOIN (
												SELECT etl_name,rundtm as previous_rundtm
												FROM (
														SELECT etl_name,rundtm,RANK() OVER (PARTITION BY etl_name ORDER BY rundtm desc) rnk
														FROM (SELECT DISTINCT etl_name,rundtm FROM z_log) subqry
														WHERE etl_name='new_high_177'
														) L
												WHERE rnk=2
												) prv ON (A.etl_name = prv.etl_name and A.rundtm = prv.previous_rundtm)
										WHERE A.etl_name='new_high_177' 

				) prev_run ON ( Z.etl_name = prev_run.etl_name AND
												Z.stepnum = prev_run.stepnum AND
												Z.part = prev_run.part 
												)
				WHERE Z.etl_name='new_high_177'
				and Z.rundtm in (SELECT max(rundtm) FROM z_log WHERE etl_name='new_high_177')
				ORDER BY rundtm desc, Z.stepnum,Z.part
			"""
			zsql = zsql.replace('new_high_177',etl_name)
			self.sqldb.RowsAffected = self.sqldb.cur.execute(zsql)
			result_html += self.sqldb.tableout(self.sqldb.cur,False,False,False)

		self.sqldb.dbdone()

		result_html += render_template('footer.html') 
		return result_html
