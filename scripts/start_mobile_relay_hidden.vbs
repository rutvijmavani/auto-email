' Launches run_mobile_relay_loop.bat with no visible console window.
' Place a shortcut to THIS file in the Windows Startup folder
' (shell:startup) so it runs automatically at login.
Set WshShell = CreateObject("WScript.Shell")
WshShell.Run """C:\Downloads\mail\scripts\run_mobile_relay_loop.bat""", 0, False
