CREATE TABLE earningsdate (dtm TIMESTAMP(6) WITHOUT TIME ZONE, 
stock CHARACTER VARYING(10) DEFAULT NULL::character varying, 
date_of_earnings DATE);
COMMENT ON TABLE earningsdate IS 'earnings dates';
