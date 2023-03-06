/*
  -- Dave Skura, 2022

*/

DROP TABLE IF EXISTS thistable;

CREATE TABLE thistable AS
SELECT CURRENT_DATE as rightnow
WHERE 1 = 1;

SELECT *
FROM thistable;