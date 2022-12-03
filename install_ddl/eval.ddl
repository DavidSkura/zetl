/*
  Dave Skura, May,2020

  dependant on: listtablesandviews

*/


DROP FUNCTION if exists eval (sql text);
CREATE OR REPLACE FUNCTION eval (sql text)
RETURNS boolean AS '
declare
	total boolean  ;
BEGIN
    EXECUTE sql INTO total;
    RETURN total;
END;
' 
 LANGUAGE plpgsql;

