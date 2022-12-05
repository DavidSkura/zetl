CREATE TABLE z_etl_running (
	etl_name varchar(250), 
	status varchar(250), 
	parameter1 varchar(250), 
	dtm TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
