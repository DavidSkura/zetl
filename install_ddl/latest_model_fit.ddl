CREATE TABLE latest_model_fit (
	stock varchar NOT NULL, 
	log_dt date NOT NULL, 
	fit_stock varchar NOT NULL, 
	fit_log_dt date NOT NULL,
	total_variance NUMERIC, 
	avg_trading_at NUMERIC,
	fit_label varchar NOT NULL default 'day',
	PRIMARY KEY(stock,log_dt,fit_stock,fit_log_dt)
);
