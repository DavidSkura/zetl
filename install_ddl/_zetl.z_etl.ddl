CREATE TABLE _zetl.z_etl (
	etl_name varchar(100) DEFAULT NULL, 
	stepnum NUMERIC(10,2) DEFAULT NULL, 
	steptablename varchar(250) DEFAULT '', 
	table_or_view varchar(25) DEFAULT 'Table', 
	estrowcount BIGINT, sqlfile varchar(250) DEFAULT '', 
	sql_to_run varchar(12000) DEFAULT '', 
	note varchar(1024) DEFAULT '', 
	dtm TIMESTAMP DEFAULT CURRENT_TIMESTAMP 
);

COMMENT ON TABLE _zetl.z_etl IS 'This is the Master etl table.  When calling run_etl.py <etl_name> .. all lines matching etl_name here are executed.';
