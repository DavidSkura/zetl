"""
  Dave Skura, 2023

"""
from postgresdave_package.postgresdave import db #install pip install postgresdave-package

mydb = db()
mydb.useConnectionDetails('postgres','<your-password-here>','localhost','1532','postgres','public')


log_sql = """
					SELECT etl_name,stepnum::integer,part
							,SUBSTRING(regexp_replace(cmd_to_run, E'[\\n\\r]+', ' ', 'g' ),1,25) as cmd_to_run
							,SUBSTRING(regexp_replace(script_output, E'[\\n\\r]+', ' ', 'g' ),1,25) as script_output
							,SUBSTRING(script_error,1,25) as script_error
							,SUBSTRING(cmdfile,1,25) as cmdfile
							,dtm::timestamp(0)    
							
					FROM z_log 
					where starttime::timestamp(0) in 
							(
							SELECT max(starttime)::timestamp(0)
							FROM z_log 
							)
					"""
print(mydb.export_query_to_str(log_sql,szdelimiter='; '))

mydb.close()
