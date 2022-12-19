/*
  -- Dave Skura, 2022
*/

-- run as posgres user
CREATE USER zetluser WITH PASSWORD 'zetluser';

-- run as posgres user
CREATE DATABASE zetl_db
	WITH
	OWNER = zetluser
	TABLESPACE = pg_default
	CONNECTION LIMIT = -1
	IS_TEMPLATE = False;

-- run as zetluser 
CREATE SCHEMA _zetl;
