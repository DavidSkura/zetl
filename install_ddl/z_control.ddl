CREATE TABLE z_control (
	etl_name varchar(100) DEFAULT '', 
	parameterkey varchar(50) DEFAULT '', 
	parametervalue varchar(200) DEFAULT '', 
	dtm TIMESTAMP DEFAULT CURRENT_TIMESTAMP, 
	PRIMARY KEY (etl_name, parameterkey)
);
