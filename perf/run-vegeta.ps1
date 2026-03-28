[CmdletBinding()]
param(
    [string]$TargetBase = "http://localhost:9999/api",
    [ValidateSet("hot", "mixed")]
    [string]$Mode = "mixed",
    [int]$Rate = 800,
    [string]$Duration = "30s",
    [int]$Connections = 200,
    [string]$OutputDir = "test-logs\vegeta"
)

$ErrorActionPreference = "Stop"

$vegetaCmd = Get-Command vegeta -ErrorAction SilentlyContinue
if (-not $vegetaCmd) {
    throw "vegeta was not found in PATH. Install from https://github.com/tsenart/vegeta"
}

if (-not (Test-Path $OutputDir)) {
    New-Item -Path $OutputDir -ItemType Directory | Out-Null
}

$stamp = Get-Date -Format "yyyyMMdd-HHmmss"
$targetsFile = Join-Path $OutputDir "targets-$stamp.txt"
$resultFile = Join-Path $OutputDir "result-$stamp.bin"
$reportFile = Join-Path $OutputDir "report-$stamp.txt"
$histFile = Join-Path $OutputDir "hist-$stamp.txt"
$jsonFile = Join-Path $OutputDir "report-$stamp.json"

$lines = New-Object System.Collections.Generic.List[string]
if ($Mode -eq "hot") {
    $lines.Add("GET $TargetBase?key=Tom")
} else {
    for ($i = 0; $i -lt 70; $i++) {
        $lines.Add("GET $TargetBase?key=Tom")
    }
    for ($i = 0; $i -lt 30; $i++) {
        $key = "key$($i % 100)"
        $lines.Add("GET $TargetBase?key=$key")
    }
}

Set-Content -Path $targetsFile -Value $lines

Write-Host "Running vegeta attack..."
vegeta attack -targets $targetsFile -rate "$Rate/s" -duration $Duration -connections $Connections | Tee-Object -FilePath $resultFile | vegeta report | Tee-Object -FilePath $reportFile

vegeta report -type="hist[0,5ms,10ms,25ms,50ms,100ms,200ms,500ms,1s]" $resultFile | Tee-Object -FilePath $histFile
vegeta report -type=json $resultFile | Set-Content -Path $jsonFile

Write-Host ""
Write-Host "Artifacts:"
Write-Host "  targets: $targetsFile"
Write-Host "  binary:  $resultFile"
Write-Host "  report:  $reportFile"
Write-Host "  hist:    $histFile"
Write-Host "  json:    $jsonFile"
