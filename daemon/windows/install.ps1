param(
    [switch]$Start
)

$ErrorActionPreference = "Stop"
$RepoDir = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
$SourceBinary = Join-Path $RepoDir "target\release\paqusd.exe"
$BaseDir = Join-Path $env:LOCALAPPDATA "Paqus"
$BinDir = Join-Path $BaseDir "bin"
$DataDir = Join-Path $BaseDir "data"
$ConfigDir = Join-Path $BaseDir "config"
$LogDir = Join-Path $BaseDir "logs"
$Binary = Join-Path $BinDir "paqusd.exe"
$Config = Join-Path $ConfigDir "node.json"
$TaskName = "PaqusDaemon"
$WasRunning = $false

if ($env:OS -ne "Windows_NT") {
    throw "This installer must run on Windows."
}
if (-not (Test-Path $SourceBinary)) {
    throw "Build paqusd first: cargo build --release --locked -p full-node --bin paqusd"
}

New-Item -ItemType Directory -Force -Path $BinDir, $DataDir, $ConfigDir, $LogDir | Out-Null
$ExistingTask = Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue
if ($ExistingTask -and $ExistingTask.State -eq "Running") {
    $WasRunning = $true
    New-Item -ItemType File -Force -Path (Join-Path $DataDir "STOP") | Out-Null
    for ($Attempt = 0; $Attempt -lt 25; $Attempt++) {
        Start-Sleep -Seconds 1
        $ExistingTask = Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue
        if (-not $ExistingTask -or $ExistingTask.State -ne "Running") { break }
    }
    Stop-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue
}
Copy-Item -Force $SourceBinary $Binary

if (-not (Test-Path $Config)) {
    $NodeConfig = [ordered]@{
        db_path = $DataDir
        listen_addr = @("[::]:5555")
        rpc_addr = "127.0.0.1:6666"
        peers = @()
        peers_file = (Join-Path $DataDir "peers.json")
        gateway_url = $null
        public_addr = $null
        gateway_heartbeat_secs = 60
        shutdown_file = (Join-Path $DataDir "STOP")
        max_peers = 128
        min_relay_fee = 1
        market_fee = 1
        low_fee_expiry_secs = 1800
        mempool_expiry_secs = 86400
        wallet = $null
        miner_address = $null
        miner_secret_key = $null
        miner_min_fee_rate = $null
        mine = $false
        mine_interval_secs = 0
        mine_attempts = 100000
    }
    $Json = $NodeConfig | ConvertTo-Json -Depth 4
    $Utf8NoBom = New-Object System.Text.UTF8Encoding($false)
    [System.IO.File]::WriteAllText($Config, $Json, $Utf8NoBom)
} else {
    Write-Host "Preserving existing $Config"
}

$Arguments = "node run `"$DataDir`" --config `"$Config`""
$Action = New-ScheduledTaskAction -Execute $Binary -Argument $Arguments -WorkingDirectory $DataDir
$Trigger = New-ScheduledTaskTrigger -AtLogOn -User $env:USERNAME
$Settings = New-ScheduledTaskSettingsSet -RestartCount 3 -RestartInterval (New-TimeSpan -Minutes 1) -ExecutionTimeLimit ([TimeSpan]::Zero)
$Principal = New-ScheduledTaskPrincipal -UserId $env:USERNAME -LogonType Interactive -RunLevel Limited
$Task = New-ScheduledTask -Action $Action -Trigger $Trigger -Settings $Settings -Principal $Principal
Register-ScheduledTask -TaskName $TaskName -InputObject $Task -Force | Out-Null

Remove-Item -Force (Join-Path $DataDir "STOP") -ErrorAction SilentlyContinue
if ($Start -or $WasRunning) {
    Start-ScheduledTask -TaskName $TaskName
}

Write-Host "paqusd installed: $Binary"
Write-Host "config: $Config"
Write-Host "data: $DataDir"
if (-not $Start -and -not $WasRunning) {
    Write-Host "start with: Start-ScheduledTask -TaskName $TaskName"
}
