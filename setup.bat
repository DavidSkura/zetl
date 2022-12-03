@echo off
REM
REM  Dave Skura, 2021
REM

echo.
echo Welcome to the Setup for ZETL v2.0

echo.
echo Python 3.7.0 is proven to work.
echo You have:
python --version

echo.
echo Checking Python requirements....
echo.

echo The follow python packages are required:
echo flask
echo mysql-connector-python

echo Do you wish to validate the python modules? [1=Yes/2=No]
CHOICE /C 12 /N /M "Command to run"
IF ERRORLEVEL 2 GOTO proceedy1N
IF ERRORLEVEL 1 GOTO proceedy1Y
GOTO end

:proceedy1Y
pip show flask
pip show mysql-connector-python


echo Do you wish to Proceed? [1=Yes/2=No]
CHOICE /C 12 /N /M "Command to run"
IF ERRORLEVEL 2 GOTO end
IF ERRORLEVEL 1 GOTO proceedy1N
GOTO end

:proceedy1N

echo Checking Database connection
python.exe python_helper.py connect
echo.
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




