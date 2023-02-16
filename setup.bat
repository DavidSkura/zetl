@echo off
REM
REM  Dave Skura, 2023
REM

SET zetl_version=2.0
SET zetl_build_dt=Feb 16/2023
SET DatabaseType=Postgres

REM *** --------------------------------------------------------- ***
REM *** Update these variables for your zetl database connection
REM *** --------------------------------------------------------- ***

SET DB_USERNAME=postgres
SET DB_USERPWD=4165605869
SET DB_HOST=localhost
SET DB_PORT=1532
SET DB_NAME=postgres
SET DB_SCHEMA=weather

REM *** --------------------------------------------------------- ***
REM *** --------------------------------------------------------- ***

echo %DB_USERNAME% - %DB_HOST% - %DB_HOST% - %DB_NAME% - %DB_SCHEMA%> .connection
echo %DB_USERPWD%> .pwd

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
python.exe zetl_initdb.py connection_test
echo.

if ERRORLEVEL 1 GOTO end

echo Checking database for tables:
echo.
python.exe zetl_initdb.py check_tables_exist
echo.

if ERRORLEVEL 1 GOTO problems_creating_tables

GOTO good_end

:problems_creating_tables
echo Problems creating tables for zetl framework.
GOTO end

:good_end
echo setup complete.
:end





