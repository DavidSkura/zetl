/*
  -- Dave Skura, 2022
DB_TYPE		= Postgres

*/

SELECT concat('Default connection Postgres',VERSION()) as label;

SELECT count(*)
FROM canweather.CanadianPostalCodes;

