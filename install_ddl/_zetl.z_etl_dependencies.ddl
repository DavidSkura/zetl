CREATE TABLE _zetl.z_etl_dependencies (
	etl_name varchar(250), 
	etl_required varchar(250), 
	table_required varchar(250), 
	dtm TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
