CREATE TABLE z_etl_stage (
	etl_name varchar(100) DEFAULT NULL, 
	stepnum NUMERIC(10,2) DEFAULT NULL, 
	steptablename varchar(250) DEFAULT '', 
	estrowcount BIGINT, sqlfile varchar(250) DEFAULT '', 
	sql_to_run varchar(12000) DEFAULT '', 
	note varchar(1024) DEFAULT '', 
	dtm TIMESTAMP DEFAULT CURRENT_TIMESTAMP 
);
