@echo off
cd /d "%~dp0"
"C:\Program Files\Git\cmd\git.exe" add -A
"C:\Program Files\Git\cmd\git.exe" status
"C:\Program Files\Git\cmd\git.exe" commit -m "fix: add all source files + nixpacks.toml for Railway deployment"
"C:\Program Files\Git\cmd\git.exe" push origin master --force
echo Done.
