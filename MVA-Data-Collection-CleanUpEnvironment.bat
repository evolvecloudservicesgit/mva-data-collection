ECHO OFF

POWERSHELL.EXE -ExecutionPolicy Unrestricted -File "%~dp0/MVA-Data-Collection.ps1" -CleanUpEnvironment

PAUSE
