@echo off
REM
REM  Dave Skura, 2023
REM
REM  To run an etl job, pass the name as a parameter 
CD ..
@echo on


zetl demo1 
@echo off

CD examples

REM Notice the results of the execution are displayed on the console.  There are also stored in the z_log table

