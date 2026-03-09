@echo off
REM Preservica Mass Modify Wrapper Script
REM This script sets up the environment and runs the preservica_modify executable

setlocal enabledelayedexpansion

REM Get the directory where this script is located
set "SCRIPT_DIR=%~dp0"

REM Run the executable with all passed arguments
"%SCRIPT_DIR%preservica_modify.exe" %*

REM Exit with the same code as the executable
exit /b %errorlevel%
