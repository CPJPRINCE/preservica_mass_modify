@echo off
REM Preservica Mass Modify Windows Installer
REM This script installs the Preservica Mass Modify to the Program Files directory

setlocal enabledelayedexpansion

echo.
echo ===============================================
echo Preservica Mass Modify Installation
echo ===============================================
echo.

REM Get the directory where this script is located
set SCRIPT_DIR=%~dp0
set SCRIPT_DIR=%SCRIPT_DIR:~0,-1%

REM Define installation paths
set INSTALL_DIR=%LOCALAPPDATA%\Preservica Mass Modify
set BIN_DIR=!INSTALL_DIR!\bin

echo Installing to: !INSTALL_DIR!
echo.

REM Create installation directory
if not exist "!INSTALL_DIR!" (
    mkdir "!INSTALL_DIR!"
    echo Created installation directory
)

if not exist "!BIN_DIR!" (
    mkdir "!BIN_DIR!"
    echo Created bin directory
)

REM Copy executable and wrapper
echo Copying files...
if exist "%SCRIPT_DIR%\bin\preservica_modify.exe" (
    xcopy /Y "%SCRIPT_DIR%\bin\preservica_modify.exe" "!BIN_DIR!\" >nul
) else (
    echo ERROR: Cannot find preservica_modify.exe at %SCRIPT_DIR%\bin\preservica_modify.exe
    pause
    exit /b 1
)

if exist "%SCRIPT_DIR%\bin\preservica_modify.cmd" (
    xcopy /Y "%SCRIPT_DIR%\bin\preservica_modify.cmd" "!BIN_DIR!\" >nul
) else (
    echo ERROR: Cannot find preservica_modify.cmd at %SCRIPT_DIR%\bin\preservica_modify.cmd
    pause
    exit /b 1
)

if exist "%SCRIPT_DIR%\README.txt" (
    xcopy /Y "%SCRIPT_DIR%\README.txt" "!INSTALL_DIR!\" >nul
) else (
    echo WARNING: Cannot find README.txt
)

if exist "%SCRIPT_DIR%\LICENSE.md" (
    xcopy /Y "%SCRIPT_DIR%\LICENSE.md" "!INSTALL_DIR!\" >nul
) else (
    echo WARNING: Cannot find LICENSE.md
)

REM Add to PATH
echo Adding to PATH...
%SystemRoot%\System32\WindowsPowerShell\v1.0\powershell.exe -Command "$path = [Environment]::GetEnvironmentVariable('Path', 'User'); if ($path -notlike '*%LOCALAPPDATA%\Preservica Mass Modify\bin*') { [Environment]::SetEnvironmentVariable('Path', $path + ';%LOCALAPPDATA%\Preservica Mass Modify\bin', 'User'); Write-Host 'Added to PATH' } else { Write-Host 'Already in PATH' }"

echo.
echo ===============================================
echo Installation Complete!
echo ===============================================
echo.
echo You can now use 'preservica_modify' from the command line.
echo Note: You may need to restart your command prompt for PATH changes to take effect.
echo.
echo To get started, type: preservica_modify --help
echo.
pause
