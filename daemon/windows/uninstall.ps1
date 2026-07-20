param(
    [switch]$Purge
)

$ErrorActionPreference = "Stop"
if ($env:OS -ne "Windows_NT") {
    throw "This uninstaller must run on Windows."
}

$DaemonDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$BaseDir = Join-Path $env:LOCALAPPDATA "Paqus"
$StopScript = Join-Path $DaemonDir "stop.ps1"
if (Test-Path $StopScript) {
    & $StopScript
}
Unregister-ScheduledTask -TaskName "PaqusDaemon" -Confirm:$false -ErrorAction SilentlyContinue
Remove-Item -Force (Join-Path $BaseDir "bin\paqusd.exe") -ErrorAction SilentlyContinue

if ($Purge) {
    Remove-Item -Recurse -Force $BaseDir -ErrorAction SilentlyContinue
    Write-Host "paqusd removed with configuration and blockchain data"
} else {
    Write-Host "paqusd removed; configuration and blockchain data were preserved in $BaseDir"
}
