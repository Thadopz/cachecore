[CmdletBinding()]
param(
    [string]$ApiBase = "http://localhost:9999",
    [int]$ProfileSeconds = 20,
    [string]$OutputDir = "test-logs\pprof"
)

$ErrorActionPreference = "Stop"

if (-not (Test-Path $OutputDir)) {
    New-Item -Path $OutputDir -ItemType Directory | Out-Null
}

$stamp = Get-Date -Format "yyyyMMdd-HHmmss"
$cpuProfile = Join-Path $OutputDir "cpu-$stamp.pb.gz"
$heapProfile = Join-Path $OutputDir "heap-$stamp.pb.gz"

Write-Host "Collecting CPU profile ($ProfileSeconds s)..."
Invoke-WebRequest -Uri "$ApiBase/debug/pprof/profile?seconds=$ProfileSeconds" -OutFile $cpuProfile

Write-Host "Collecting heap profile..."
Invoke-WebRequest -Uri "$ApiBase/debug/pprof/heap" -OutFile $heapProfile

Write-Host ""
Write-Host "Saved profiles:"
Write-Host "  cpu:  $cpuProfile"
Write-Host "  heap: $heapProfile"
Write-Host ""
Write-Host "Inspect examples:"
Write-Host "  go tool pprof -http=:0 .\\server.exe $cpuProfile"
Write-Host "  go tool pprof -top .\\server.exe $heapProfile"
