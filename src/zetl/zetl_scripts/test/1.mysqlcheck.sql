/*
  -- Dave Skura, 2022
DB_TYPE		= MySQL

*/
SELECT concat('Default connection MySQL',VERSION()) as label
	,CASE 
		WHEN '<ARGV1>' = '' THEN 'No parameter Passed'
	 ELSE
	 	'This Parameter passed <ARGV1>'
	 END cmd_parm


