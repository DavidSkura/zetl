/*
  -- Dave Skura, 2023
*/

SELECT etl_name,stepnum,part
		,SUBSTRING(regexp_replace(cmd_to_run, E'[\\n\\r]+', ' ', 'g' ),1,25) as cmd_to_run
		,SUBSTRING(regexp_replace(script_output, E'[\\n\\r]+', ' ', 'g' ),1,25) as script_output
		,SUBSTRING(script_error,1,25) as script_error
		,SUBSTRING(cmdfile,1,25) as cmdfile
		,dtm::timestamp(0)    
		
FROM z_log 
ORDER BY starttime DESC
limit 10;