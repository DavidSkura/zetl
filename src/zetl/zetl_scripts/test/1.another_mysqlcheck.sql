/*
  -- Dave Skura, 2022
DB_TYPE		= MySQL
DB_USERNAME	= dave
DB_USERPWD  = **********
DB_HOST		= localhost
DB_PORT		= 3306
DB_NAME		= atlas

*/

DROP TABLE IF EXISTS test_table;

CREATE TABLE test_table (
name varchar(25),
age integer
);

INSERT INTO test_table (name,age) VALUES 
('dave',50),
('frank',75),
('Billyjoe',45);

SELECT *
FROM test_table ;

DELETE FROM test_table
WHERE name = 'dave';

DROP TABLE test_table;




