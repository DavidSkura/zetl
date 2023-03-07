/*
  -- Dave Skura, 2022

DB_TYPE		= MySQL
DB_USERNAME	= dad
DB_USERPWD  = **********
DB_HOST		= localhost
DB_PORT		= 3306
DB_NAME		= nfldb
DB_SCHEMA	= nfldb

*/

DROP TABLE IF EXISTS thistable;

CREATE TABLE thistable AS
SELECT CURRENT_DATE as rightnow
WHERE 1 = 1;

SELECT *
FROM thistable;