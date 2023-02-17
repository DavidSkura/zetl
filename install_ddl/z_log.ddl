DROP TABLE IF EXISTS z_log;

CREATE TABLE z_log (
	id SERIAL NOT NULL primary key, 
	dbuser varchar(100) DEFAULT NULL, 
	rundtm timestamp DEFAULT now()::timestamp, 
	etl_name varchar(100) DEFAULT NULL, 
	stepnum NUMERIC(5,1) DEFAULT NULL, 
	part INTEGER DEFAULT 1, 
	steptablename varchar(250) DEFAULT NULL, 
	rowcount BIGINT, 
	starttime TIMESTAMP DEFAULT now()::timestamp,  
	endtime TIMESTAMP, 
	cmd_to_run varchar(8124) DEFAULT NULL, 
	script_output varchar(8124) DEFAULT NULL, 
	script_error varchar(255) DEFAULT NULL, 
	cmdfile varchar(250) DEFAULT NULL, 
	database varchar(250) DEFAULT NULL, 
	dtm TIMESTAMP DEFAULT now()::timestamp
);

COMMENT ON TABLE z_log IS 'Global log table.  Every sql which is run from zetl.py is logged here.';
