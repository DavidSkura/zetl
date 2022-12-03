CREATE TABLE major_holders (
	dtm TIMESTAMP(6) WITHOUT TIME ZONE, 
	stock CHARACTER VARYING(10), 
	all_insider DOUBLE PRECISION, 
	shares_held_by_institutions DOUBLE PRECISION, 
	float_held_by_institutions DOUBLE PRECISION, 
	number_of_institutions INTEGER
);
