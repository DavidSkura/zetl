/*
  Dave Skura, 2021

	DELETE FROM z_etl WHERE etl_name='ETL_JOB_NAME';

	INSERT INTO z_etl (etl_name,stepnum,steptablename,estrowcount,sqlfile) VALUES
	('ETL_JOB_NAME',0,'stock_daily_mvmnt',0,'C:\\stocks\\ETL_JOB_NAME\\0.ETL_JOB_NAME.sql');

	SELECT *
	FROM z_etl
	WHERE etl_name='ETL_JOB_NAME';


	-- --------------------------------------------------------
	-- Dependencies
	-- --------------------------------------------------------

	DELETE FROM z_etl_dependencies WHERE etl_name='ETL_JOB_NAME';

	INSERT INTO z_etl_dependencies (etl_name,etl_required,table_required) VALUES
	('ETL_JOB_NAME','raw','stockdata'),
	('ETL_JOB_NAME','raw','activity'),
	('ETL_JOB_NAME','raw','etl_running');

	SELECT *
	FROM z_etl_dependencies
	WHERE etl_name='ETL_JOB_NAME';



*/


UPDATE activity	
SET previously=currently,dtm=CURRENT_TIMESTAMP,	prvkeyfld=keyfld, 
	currently='*holdoff*', keyfld='ETL_JOB_NAME running';

DELETE FROM etl_running WHERE etl_name='ETL_JOB_NAME';
INSERT INTO etl_running (etl_name,status,parameter1) VALUES ('ETL_JOB_NAME','Running',60);


/*

Put sql work here

*/



DELETE FROM etl_running WHERE etl_name='ETL_JOB_NAME';
INSERT INTO etl_running (etl_name,status) VALUES ('ETL_JOB_NAME','Completed');

UPDATE activity	
SET previously=currently,dtm=CURRENT_TIMESTAMP,	prvkeyfld=keyfld, 
	currently='IDLE', keyfld='';
