@echo off
cd /d "%~dp0"

:: 清掉還佔著埠號的舊程序（3021 = 後端、5021 = 前端 dev server）
for %%P in (3021 5021) do (
    for /f "tokens=5" %%A in ('netstat -ano ^| findstr ":%%P .*LISTENING"') do (
        taskkill /F /PID %%A >nul 2>&1
    )
)

:: 透過 Python launcher 啟動
python launcher.py
