@echo off

::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
:: Set file name
set file_name=resa_uv_abs.py

:: Set path to Mamba
set root=C:/ProgramData/mambaforge

:: (Optional). Set path to 64-bit Oracle Instant Client
::set PATH=C:/oracle/instantclient_19_12;%PATH%
::::::::::::::::::::::::::::::::::::::::::::::::::::::::::

echo Activating environment.
call %root%/Scripts/activate.bat %root% || goto :error
call activate resa2_uvabs || goto :error

echo Updating code.
call git pull

echo Running script.
cd /d %~dp0/notebooks || goto :error
python %file_name% || goto :error
cd /d %~dp0/
echo Completed successfully!
goto :EOF

:error
echo Failed with error #%errorlevel%.
exit /b %errorlevel%