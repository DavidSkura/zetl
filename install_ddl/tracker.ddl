CREATE TABLE tracker (
	stock varchar, 
	buy_units integer, 
	buy_price numeric, 
	buy_dtm TIMESTAMP(6) WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
	sell_units integer, 
	sell_price numeric, 
	sell_dtm TIMESTAMP(6) WITHOUT TIME ZONE
);

