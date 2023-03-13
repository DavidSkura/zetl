/*
  -- Dave Skura, 2022
DB_TYPE		= MySQL
DB_USERNAME	= dad
DB_USERPWD  = **********
DB_HOST		= localhost
DB_PORT		= 3306
DB_NAME		= atlas

*/
SELECT concat('New connection as dad to MySQL',VERSION()) as label;

SELECT count(*)
FROM CanadianPostalCodes;

