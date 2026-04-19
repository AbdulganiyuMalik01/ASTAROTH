@echo off
cd /d "%~dp0"
rmdir /s /q ".git"
"C:\Program Files\Git\cmd\git.exe" init
"C:\Program Files\Git\cmd\git.exe" add -A
"C:\Program Files\Git\cmd\git.exe" status
