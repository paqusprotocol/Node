$ErrorActionPreference = "Stop"
$TaskName = "PaqusDaemon"
$DataDir = Join-Path $env:LOCALAPPDATA "Paqus\data"
$Task = Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue
if (-not $Task -or $Task.State -ne "Running") {
    Write-Host "paqusd is not running"
    exit 0
}

New-Item -ItemType Directory -Force -Path $DataDir | Out-Null
New-Item -ItemType File -Force -Path (Join-Path $DataDir "STOP") | Out-Null
for ($Attempt = 0; $Attempt -lt 25; $Attempt++) {
    Start-Sleep -Seconds 1
    $Task = Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue
    if (-not $Task -or $Task.State -ne "Running") {
        Write-Host "paqusd stopped cleanly"
        exit 0
    }
}

Stop-ScheduledTask -TaskName $TaskName
Write-Warning "paqusd did not exit within 25 seconds and the scheduled task was stopped"
