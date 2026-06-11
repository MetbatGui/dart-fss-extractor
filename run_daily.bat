@echo off
echo ===================================================
echo  Starting DART Financial Data Daily Collection
echo ===================================================

echo Running scheduler...
uv run src/daily_scheduler.py

if %errorlevel% neq 0 (
    echo [ERROR] Daily collection failed with exit code %errorlevel%.
    pause
    exit /b %errorlevel%
)

echo [SUCCESS] Daily collection completed successfully.
pause
