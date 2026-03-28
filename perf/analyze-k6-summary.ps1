[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [string]$SummaryPath,
    [double]$P95BudgetMs = 10,
    [double]$P99BudgetMs = 30,
    [double]$ErrorBudgetRate = 0.001
)

$ErrorActionPreference = "Stop"

if (-not (Test-Path $SummaryPath)) {
    throw "Summary file not found: $SummaryPath"
}

$summary = Get-Content -Path $SummaryPath -Raw | ConvertFrom-Json
$metrics = $summary.metrics

function Get-MetricValue {
    param(
        [Parameter(Mandatory = $true)]
        $Metric,
        [Parameter(Mandatory = $true)]
        [string]$Name,
        $Default = $null
    )

    if ($null -eq $Metric) {
        return $Default
    }

    $prop = $Metric.PSObject.Properties[$Name]
    if ($null -eq $prop) {
        return $Default
    }

    return $prop.Value
}

$reqRate = [double](Get-MetricValue -Metric $metrics.http_reqs -Name "rate" -Default 0)
$reqCount = [double](Get-MetricValue -Metric $metrics.http_reqs -Name "count" -Default 0)
$avg = [double](Get-MetricValue -Metric $metrics.http_req_duration -Name "avg" -Default 0)
$p95 = [double](Get-MetricValue -Metric $metrics.http_req_duration -Name "p(95)" -Default 0)
$p99Raw = Get-MetricValue -Metric $metrics.http_req_duration -Name "p(99)" -Default $null
$expectedMetric = $metrics.'http_req_duration{expected_response:true}'
if ($null -eq $p99Raw) {
    $p99Raw = Get-MetricValue -Metric $expectedMetric -Name "p(99)" -Default $null
}
$hasP99 = $null -ne $p99Raw
$p99 = if ($hasP99) { [double]$p99Raw } else { 0 }
$p99Text = if ($hasP99) { "{0:N2}ms" -f $p99 } else { "N/A" }

if (-not $hasP99) {
    Write-Warning "p99 is missing in this k6 summary export, p99 budget check will be skipped."
}
$max = [double](Get-MetricValue -Metric $metrics.http_req_duration -Name "max" -Default 0)
$errRate = [double](Get-MetricValue -Metric $metrics.http_req_failed -Name "value" -Default 0)
$checksPass = [double](Get-MetricValue -Metric $metrics.checks -Name "passes" -Default 0)
$checksFail = [double](Get-MetricValue -Metric $metrics.checks -Name "fails" -Default 0)

$p95Ok = $p95 -lt $P95BudgetMs
$p99Ok = if ($hasP99) { $p99 -lt $P99BudgetMs } else { $true }
$errOk = $errRate -lt $ErrorBudgetRate
$checksOk = $checksFail -eq 0

Write-Host ""
Write-Host "=== k6 Summary Analysis ==="
Write-Host ("requests:  {0:N0}" -f $reqCount)
Write-Host ("throughput:{0,9:N2} req/s" -f $reqRate)
Write-Host ("latency:   avg={0:N2}ms p95={1:N2}ms p99={2} max={3:N2}ms" -f $avg, $p95, $p99Text, $max)
Write-Host ("checks:    pass={0:N0} fail={1:N0}" -f $checksPass, $checksFail)
Write-Host ("errorRate: {0:P4}" -f $errRate)
Write-Host ""

Write-Host "Budgets:"
Write-Host ("  p95 < {0}ms => {1}" -f $P95BudgetMs, $(if ($p95Ok) { "PASS" } else { "FAIL" }))
Write-Host ("  p99 < {0}ms => {1}" -f $P99BudgetMs, $(if ($p99Ok) { "PASS" } else { "FAIL" }))
Write-Host ("  error rate < {0:P4} => {1}" -f $ErrorBudgetRate, $(if ($errOk) { "PASS" } else { "FAIL" }))
Write-Host ("  checks all pass => {0}" -f $(if ($checksOk) { "PASS" } else { "FAIL" }))

if ($p95Ok -and $p99Ok -and $errOk -and $checksOk) {
    Write-Host ""
    Write-Host "Overall: PASS" -ForegroundColor Green
} else {
    Write-Host ""
    Write-Host "Overall: FAIL" -ForegroundColor Red
}

if ($max -gt ($p99 * 3)) {
    Write-Host "Hint: max latency is much higher than p99, consider checking GC/lock/contention with pprof." -ForegroundColor Yellow
}
if (-not $hasP99) {
    Write-Host "Hint: current summary lacks p99, so p99 budget was skipped for this run." -ForegroundColor Yellow
}
