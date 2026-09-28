@echo off
REM Auto-restarting launcher for the mobile relay SOCKS5 proxy.
REM Started hidden at Windows login via a shortcut in the Startup folder
REM (scripts/mobile_relay_socks5.py itself refuses to bind to 0.0.0.0, so
REM this stays safe even if something here is misconfigured).
cd /d C:\Downloads\mail
:loop
pythonw scripts\mobile_relay_socks5.py
timeout /t 60 /nobreak >nul
goto loop
