/*
  -- Dave Skura, 2022


*/

SELECT 'Default connection sqlite '||sqlite_version() as label
	,CASE 
		WHEN '<ARGV1>' = '' THEN 'No parameter Passed'
	 ELSE
	 	'This Parameter passed <ARGV1>'
	 END cmd_parm;

CREATE TABLE test_table (
name varchar(25),
age integer
);

INSERT INTO test_table (name,age) VALUES 
('dave',50),
('frank',75),
('Billyjoe',45);

SELECT *
FROM test_table ;

DELETE FROM test_table
WHERE name = 'dave';

DROP TABLE test_table;


