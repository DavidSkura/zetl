@echo off
REM
REM  Dave Skura, 2023
REM
SET ERRORLEVEL=0

echo  hello from bat

if  errorlevel 1 goto ERROR
echo - No issue
goto EOF 

:ERROR
echo - Call failed
cmd /k
exit /b 1

:EOF 
exit /b 0