/*
  -- Dave Skura, 2022

*/
SELECT 'Default connection sqlite '||sqlite_version() as label
	,CASE 
		WHEN '<ARGV1>' = '' THEN 'No parameter Passed'
	 ELSE
	 	'This Parameter passed <ARGV1>'
	 END cmd_parm


