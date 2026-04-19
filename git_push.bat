@echo off
cd /d "%~dp0"
"C:\Program Files\Git\cmd\git.exe" config user.email "maliksucess@gmail.com"
"C:\Program Files\Git\cmd\git.exe" config user.name "AbdulganiyuMalik01"
"C:\Program Files\Git\cmd\git.exe" remote add origin https://github.com/AbdulganiyuMalik01/ASTAROTH.git
"C:\Program Files\Git\cmd\git.exe" commit -m "feat: implement all 11 improvements (soft gates, fallback APIs, Nitter rotation, circuit breaker env config, perf tracking, Telegram commands, queue backpressure, cross-cooldown, cleanup worker)"
"C:\Program Files\Git\cmd\git.exe" branch -M main
"C:\Program Files\Git\cmd\git.exe" push -u origin main
