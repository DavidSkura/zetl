@echo off
REM
REM  Dave Skura, 2023
REM
REM  pip install zetl

echo  Backing up tables in MySQL

call py -m zetl.sqlite_extract "SELECT * FROM z_etl" z_etl.csv
call py -m zetl.sqlite_extract "SELECT * FROM z_log" z_log.csv


