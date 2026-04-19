@echo off
cd /d "%~dp0"
"C:\Program Files\Git\cmd\git.exe" branch -M master
"C:\Program Files\Git\cmd\git.exe" push -u origin master --force
