/*
  -- Dave Skura, 2022

DB_USERNAME	= zetluser
DB_USERPWD  = zetluser
DB_HOST		= localhost
DB_PORT		= 1532
DB_NAME		= romania
DB_SCHEMA	= public

*/

DROP TABLE IF EXISTS thistable;

CREATE TABLE thistable AS
SELECT CURRENT_DATE as rightnow
WHERE 1 = 1;
