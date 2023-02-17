@echo off
REM
REM  Dave Skura, 2023
REM
REM  To see all the files for an etl job, just look at the folder
CD ..\zetl_scripts\demo1
@echo on


DIR /ON
@echo off
CD ..\..\examples

REM Notice the files start with a number.  This is the sequence they will be executed.
REM you can ignore the z_etl.csv file.  It gets created to show what has been loaded to the zetl database
