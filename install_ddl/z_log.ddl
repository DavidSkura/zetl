CREATE TABLE z_log (
	id SERIAL NOT NULL, 
	dbuser varchar(100) DEFAULT NULL, 
	rundtm TIMESTAMP, 
	etl_name varchar(100) DEFAULT NULL, 
	stepnum NUMERIC(5,1) DEFAULT NULL, 
	part INTEGER DEFAULT 1, 
	steptablename varchar(250) DEFAULT NULL, 
	rows_affected BIGINT, 
	rowcount BIGINT, 
	starttime TIMESTAMP DEFAULT CURRENT_TIMESTAMP, 
	endtime TIMESTAMP, 
	sql_to_run varchar(15000) DEFAULT NULL, 
	sql_error varchar(255) DEFAULT NULL, 
	sqlfile varchar(250) DEFAULT NULL, 
	dtm TIMESTAMP DEFAULT CURRENT_TIMESTAMP, 
	line varchar(200) DEFAULT NULL, 
	PRIMARY KEY (id)
);

COMMENT ON TABLE z_log IS 'Global log table.  Every sql which is run from zetl.py is logged here.';