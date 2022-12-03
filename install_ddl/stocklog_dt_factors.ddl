CREATE TABLE stocklog_dt_factors (
	stock varchar NOT NULL, 
	log_dt DATE  NOT NULL, 
	hrs INTEGER, 
	mins INTEGER, 
	now_trading_at NUMERIC, 
	factor NUMERIC, 
	avg_trading_at NUMERIC,
	PRIMARY KEY(stock,log_dt,hrs,mins)
);
