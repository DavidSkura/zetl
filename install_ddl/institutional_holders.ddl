CREATE TABLE institutional_holders (dtm TIMESTAMP(6) WITHOUT TIME ZONE, 
stock CHARACTER VARYING(10) DEFAULT NULL::character varying, 
id INTEGER, 
holder CHARACTER VARYING(150) DEFAULT NULL::character varying, 
shares DOUBLE PRECISION, 
date_reported DATE, 
pct_out DOUBLE PRECISION, 
value BIGINT);
COMMENT ON TABLE institutional_holders IS 'vertical table, 
institutional holders';
