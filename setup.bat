@echo off
REM
REM  Dave Skura, 2023
REM

SET zetl_version=3.0
SET zetl_build_dt=Feb 16/2023
SET DatabaseType=Postgres

echo Welcome to the Setup for %DatabaseType% zetl v%zetl_version%
echo.
echo Python 3.7.0 is proven to work.
echo.
echo You have:
python --version

pip install mysql-connector-python
pip install psycopg2

SET DB_HOST=localhost
SET DB_PORT=1532
SET DB_NAME=postgres
SET DB_SCHEMA=public
SET DB_USERNAME=postgres
SET DB_USERPWD=password

echo you need to enter the database connection details or accept defaults:

set /P DB_HOST=Enter DB_HOST (localhost): 
set /P DB_PORT=Enter DB_PORT (1532): 
set /P DB_NAME=Enter DB_NAME (postgres): 
set /P DB_SCHEMA=Enter DB_SCHEMA (public): 
set /P DB_USERNAME=Enter DB_USERNAME (postgres): 
set /P DB_USERPWD=Enter DB_USERPWD (password): 

echo %DB_USERNAME% - %DB_HOST% - %DB_PORT% - %DB_NAME% - %DB_SCHEMA%> .connection
echo %DB_USERPWD%> .pwd

echo Checking Database connection
zetl_initdb.py connection_test
echo.

if ERRORLEVEL 1 GOTO end

echo Checking database for tables:
echo.
zetldbfile.py check_tables_exist
echo.

if ERRORLEVEL 1 GOTO problems_creating_tables

GOTO good_end

:problems_creating_tables
echo Problems creating tables for zetl framework.
GOTO end

:good_end
echo setup complete.
:end





