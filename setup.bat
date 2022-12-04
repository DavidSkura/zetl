@echo off
REM
REM  Dave Skura, 2022
REM

SET zetl_version=1.0
SET zetl_build_dt=Dec 2/2022
SET DatabaseType=Postgres
SET python_database_file=postgresdatabase.py

echo dbvars.py will be overwritten.

echo Do you wish to proceed? [1=Yes/2=No]
CHOICE /C 12 /N /M "Command to run"
IF ERRORLEVEL 2 GOTO end
IF ERRORLEVEL 1 GOTO overwrite_it
GOTO end

:overwrite_it

SET DB_USERNAME=dave
SET DB_USERPWD=dave
SET DB_HOST=localhost
SET DB_PORT=1532
SET DB_NAME=postgres
SET DB_SCHEMA=_keydemo
SET dbconnectionstr=%DB_USERNAME%/%DB_USERPWD%@%DB_HOST%:%DB_PORT%/%DB_NAME%/%DB_SCHEMA%

echo class dbinfo: > dbvars.py
echo		def __init__(self): >> dbvars.py
echo			self.version=%zetl_version% >> dbvars.py
echo			self.DatabaseType='%DatabaseType%' >> dbvars.py
echo			self.updated='%zetl_build_dt%' >> dbvars.py
echo: >> dbvars.py
echo			self.DB_USERNAME='%DB_USERNAME%' >> dbvars.py
echo			self.DB_USERPWD='%DB_USERPWD%' >> dbvars.py
echo			self.DB_HOST='%DB_HOST%' >> dbvars.py
echo			self.DB_PORT='%DB_PORT%' >> dbvars.py
echo			self.DB_NAME='%DB_NAME%' >> dbvars.py
echo			self.DB_SCHEMA='%DB_SCHEMA%' >> dbvars.py
echo			self.dbconnectionstr='%dbconnectionstr%' >> dbvars.py

echo.

echo Welcome to the Setup for %Database% zetl v%zetl_version%
echo Database Connection variables set as follows: %dbconnectionstr%
echo.
echo Python 3.7.0 is proven to work.
echo.
echo You have:
python --version

echo.
echo Checking Python requirements....
echo.

echo The python package psycopg2 is required

echo Do you wish to validate python? [1=Yes/2=No]
CHOICE /C 12 /N /M "Command to run"
IF ERRORLEVEL 2 GOTO proceedy1N
IF ERRORLEVEL 1 GOTO proceedy1Y
GOTO end

:proceedy1Y
pip show psycopg2

echo Do you wish to Proceed? [1=Yes/2=No]
CHOICE /C 12 /N /M "Command to run"
IF ERRORLEVEL 2 GOTO end
IF ERRORLEVEL 1 GOTO PythonIsGoodIGuess
GOTO end

:PythonIsGoodIGuess
:proceedy1N

echo Checking Database connection
python.exe %python_database_file% 
echo.

if ERRORLEVEL 1 GOTO end

GOTO end

echo Checking database for tables:
echo.
python.exe python_helper.py check_tables_exist
echo.

echo Do you wish to create the views ?? [1=Yes/2=No]
CHOICE /C 12 /N /M "Command to run"
IF ERRORLEVEL 2 GOTO proceedy3N
IF ERRORLEVEL 1 GOTO proceedy3Y
GOTO end

:proceedy3Y
python.exe python_helper.py create_views

:proceedy3N

echo Do you wish to create the functions ?? [1=Yes/2=No]
CHOICE /C 12 /N /M "Command to run"
IF ERRORLEVEL 2 GOTO proceedy4N
IF ERRORLEVEL 1 GOTO proceedy4Y
GOTO end

:proceedy4Y
python.exe python_helper.py create_functions

:proceedy4N

:proceedy2Y
python.exe python_helper.py check_huge_tables_exist

:proceedy2N
echo done.


:end




