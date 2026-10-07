@echo off
cd /d "%~dp0"
start "" "%~dp0my_messenger_server.exe"
timeout /t 2 /nobreak >nul
start "" "%~dp0my_messenger_client.exe"