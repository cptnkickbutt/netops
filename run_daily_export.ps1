# run_daily_export.ps1
$ErrorActionPreference = "Stop"

# Ensure working dir so .env is found (and any relative paths behave)
Set-Location "C:\Users\eshor\Downloads\netops_package"

# Optional: log folder
$logDir = "C:\Users\eshor\Downloads\netops_logs"
New-Item -ItemType Directory -Force -Path $logDir | Out-Null
$logFile = Join-Path $logDir ("daily-export_{0}.log" -f (Get-Date -Format "yyyyMMdd_HHmmss"))

# Run the command and capture stdout/stderr to a log
& netops daily-export *>> $logFile

exit $LASTEXITCODE
