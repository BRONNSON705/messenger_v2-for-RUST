@echo off
cd /d "%~dp0"
call cargo build --release
if errorlevel 1 (
    echo Ошибка сборки!
    pause
    exit /b 1
)
if not exist "%~dp0app" mkdir "%~dp0app"
copy /Y "%~dp0target\release\my_messenger_server.exe" "%~dp0app\" >nul
copy /Y "%~dp0target\release\my_messenger_client.exe" "%~dp0app\" >nul
if not exist "%~dp0app\my_messenger.db" if exist "%~dp0my_messenger.db" copy /Y "%~dp0my_messenger.db" "%~dp0app\" >nul
echo.
echo Готово! Запускай двойным кликом: app\start.bat
pause