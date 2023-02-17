@echo off
REM
REM  Dave Skura, 2023
REM
REM  call zetl.py on it's own to see a list of etl jobs ready to run
CD ..
@echo on


zetl
@echo off
CD examples

REM notice the etl jobs match folder names under zetl_scripts
