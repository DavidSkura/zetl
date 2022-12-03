CREATE TABLE stockdata (
	stock CHARACTER VARYING(25) DEFAULT NULL::character varying NOT NULL, 
	log_dt DATE NOT NULL, 
	open NUMERIC(18,2) DEFAULT NULL::numeric, 
	high NUMERIC(18,2) DEFAULT NULL::numeric, 
	low NUMERIC(18,2) DEFAULT NULL::numeric, 
	close NUMERIC(18,2) DEFAULT NULL::numeric, 
	adjclose NUMERIC(18,2) DEFAULT NULL::numeric, 
	volume NUMERIC(18,2) DEFAULT NULL::numeric, 
	update_dtm TIMESTAMP(6) WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP, 
	PRIMARY KEY (stock, log_dt)
);
COMMENT ON TABLE stockdata IS 'Master Stockdata table, contains all stocks with history and detail available';
