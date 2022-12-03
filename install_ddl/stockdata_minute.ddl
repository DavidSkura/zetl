CREATE TABLE stockdata_minute (
	stock CHARACTER VARYING, 
	log_dt DATE, 
	log_dtm TIMESTAMP(6) WITHOUT TIME ZONE, 
	open NUMERIC, 
	high NUMERIC, 
	low NUMERIC, 
	close NUMERIC, 
	volume NUMERIC,
	PRIMARY KEY (stock, log_dt, log_dtm)
);
