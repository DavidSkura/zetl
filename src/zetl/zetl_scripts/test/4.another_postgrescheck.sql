/*
  -- Dave Skura, 2022
DB_TYPE		= Postgres
DB_USERNAME	= dad
DB_USERPWD  = ***
DB_HOST		= localhost
DB_PORT		= 1532
DB_NAME		= nfl
DB_SCHEMA	= _consume

*/

SELECT concat('new connection as to Postgres',VERSION()) as label;

SELECT count(*)
FROM _consume.seasons;

