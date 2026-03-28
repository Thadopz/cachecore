[CmdletBinding()]
param(
    [string]$BaseUrl = "http://localhost:9999",
    [ValidateSet("hot", "mixed")]
    [string]$Mode = "mixed",
    [int]$VUs = 80,
    [string]$Duration = "60s",
    [int]$SeededMax = 100,
    [string]$ValidKeys = "Tom",
    [switch]$MixedIncludeSeeded,
    [double]$SleepSeconds = 0,
    [double]$ThresholdP95Ms = 10,
    [double]$ThresholdP99Ms = 30,
    [double]$ThresholdErrorRate = 0.001,
    [switch]$FailOnThreshold,
    [switch]$SkipAnalyze,
    [string]$OutputDir = "test-logs\k6"
)

$ErrorActionPreference = "Stop"

# Windows PowerShell 5.1 uses legacy code pages by default.
# Force UTF-8 to prevent mojibake for k6 unicode symbols in console/log.
$oldConsoleOut = [Console]::OutputEncoding
$oldOutputEncoding = $OutputEncoding
try {
    cmd /c chcp 65001 > $null
} catch {
}
$utf8NoBom = New-Object System.Text.UTF8Encoding($false)
[Console]::OutputEncoding = $utf8NoBom
$OutputEncoding = $utf8NoBom

$gopath = go env GOPATH
$k6Local = Join-Path $gopath "bin\k6.exe"
$k6Cmd = Get-Command k6 -ErrorAction SilentlyContinue
if ($k6Cmd) {
    $k6Exe = $k6Cmd.Source
} elseif (Test-Path $k6Local) {
    $k6Exe = $k6Local
} else {
    throw "k6 not found. Install with: go install go.k6.io/k6@latest"
}

if (-not (Test-Path $OutputDir)) {
    New-Item -Path $OutputDir -ItemType Directory | Out-Null
}

$stamp = Get-Date -Format "yyyyMMdd-HHmmss"
$summaryPath = Join-Path $OutputDir "summary-$stamp.json"
$logPath = Join-Path $OutputDir "output-$stamp.log"
$scriptPath = Join-Path $PSScriptRoot "k6-script.js"
$analyzeScriptPath = Join-Path $PSScriptRoot "analyze-k6-summary.ps1"

if (-not (Test-Path $scriptPath)) {
    throw "k6 script file not found: $scriptPath"
}

Write-Host "Using k6 binary: $k6Exe"
& $k6Exe version

$env:BASE_URL = $BaseUrl
$env:MODE = $Mode
$env:VUS = "$VUs"
$env:DURATION = $Duration
$env:SEEDED_MAX = "$SeededMax"
$env:VALID_KEYS = $ValidKeys
$env:MIXED_INCLUDE_SEEDED = if ($MixedIncludeSeeded) { "true" } else { "false" }
$env:SLEEP_SECONDS = "$SleepSeconds"
$env:THRESHOLD_P95_MS = "$ThresholdP95Ms"
$env:THRESHOLD_P99_MS = "$ThresholdP99Ms"
$env:THRESHOLD_ERROR_RATE = "$ThresholdErrorRate"
$env:K6_NO_COLOR = "1"

$previousEAP = $ErrorActionPreference
$ErrorActionPreference = "Continue"
$k6Lines = & $k6Exe run --no-color --summary-export $summaryPath $scriptPath 2>&1
$k6ExitCode = $LASTEXITCODE
$ErrorActionPreference = $previousEAP

$k6Lines | ForEach-Object { Write-Host $_ }
$k6Lines | Set-Content -Path $logPath -Encoding UTF8

if ($k6ExitCode -ne 0) {
    Write-Warning "k6 exited with code $k6ExitCode (typically thresholds crossed)."
}

if (-not $SkipAnalyze) {
    if (Test-Path $analyzeScriptPath) {
        & $analyzeScriptPath -SummaryPath $summaryPath -P95BudgetMs $ThresholdP95Ms -P99BudgetMs $ThresholdP99Ms -ErrorBudgetRate $ThresholdErrorRate
    } else {
        Write-Warning "Analyze script not found: $analyzeScriptPath"
    }
}

Write-Host ""
Write-Host "Artifacts:"
Write-Host "  summary: $summaryPath"
Write-Host "  log:     $logPath"

[Console]::OutputEncoding = $oldConsoleOut
$OutputEncoding = $oldOutputEncoding

if ($FailOnThreshold -and $k6ExitCode -ne 0) {
    exit $k6ExitCode
}
