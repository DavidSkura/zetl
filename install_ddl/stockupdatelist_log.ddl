CREATE TABLE stockupdatelist_log (
	stock CHARACTER VARYING(10) DEFAULT NULL::character varying, 
	attempted_update_dtm TIMESTAMP(6) WITHOUT TIME ZONE
);
COMMENT ON TABLE stockupdatelist_log IS 'Which stocks were updated';
