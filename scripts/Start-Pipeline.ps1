#Requires -Version 7.2
<#
.SYNOPSIS
  Triggers the ADF pipeline and waits for it, printing activity-level errors on failure.
  Reads resource names from the Terraform platform outputs.

.EXAMPLE
  ./scripts/Start-Pipeline.ps1 -ReportingYear 2024
#>
[CmdletBinding()]
param(
    [string] $ReportingYear = '2024',
    [int] $PollSeconds = 30
)

$ErrorActionPreference = 'Stop'
$repo = Split-Path -Parent $PSScriptRoot
$p = terraform -chdir="$(Join-Path $repo 'infra/terraform/platform')" output -json | ConvertFrom-Json
if ($LASTEXITCODE -ne 0) { throw 'Could not read Terraform outputs. Run Deploy-All.ps1 first.' }

$sub = az account show --query id -o tsv
$factoryUri = "https://management.azure.com/subscriptions/$sub/resourceGroups/$($p.resource_group_name.value)/providers/Microsoft.DataFactory/factories/$($p.data_factory_name.value)"
$api = 'api-version=2018-06-01'

$body = @{ reportingYear = $ReportingYear } | ConvertTo-Json -Compress
$bodyFile = New-TemporaryFile
Set-Content $bodyFile $body -Encoding utf8

$run = az rest --method post --uri "$factoryUri/pipelines/pl_tri_lakehouse/createRun?$api" --body "@$bodyFile" | ConvertFrom-Json
Remove-Item $bodyFile
if (-not $run.runId) { throw 'createRun did not return a runId' }
Write-Host "Started ADF run $($run.runId) for reporting year $ReportingYear"
Write-Host "Watch it live: ADF Studio > Monitor > Pipeline runs"

$started = Get-Date
do {
    Start-Sleep -Seconds $PollSeconds
    $status = az rest --method get --uri "$factoryUri/pipelineruns/$($run.runId)?$api" | ConvertFrom-Json
    $elapsed = [int]((Get-Date) - $started).TotalMinutes
    Write-Host ("[{0,3} min] {1}" -f $elapsed, $status.status)
} while ($status.status -in 'Queued', 'InProgress', 'Canceling')

if ($status.status -eq 'Succeeded') {
    Write-Host "`nPipeline succeeded in $([int]$status.durationInMs/60000) min." -ForegroundColor Green
    exit 0
}

Write-Host "`nPipeline ended: $($status.status) - $($status.message)" -ForegroundColor Red
$filter = @{
    lastUpdatedAfter  = $started.ToUniversalTime().AddMinutes(-5).ToString('o')
    lastUpdatedBefore = (Get-Date).ToUniversalTime().AddMinutes(5).ToString('o')
} | ConvertTo-Json -Compress
$filterFile = New-TemporaryFile
Set-Content $filterFile $filter -Encoding utf8
$acts = az rest --method post --uri "$factoryUri/pipelineruns/$($run.runId)/queryActivityruns?$api" --body "@$filterFile" | ConvertFrom-Json
Remove-Item $filterFile
$acts.value | Where-Object status -ne 'Succeeded' |
    Select-Object activityName, status, @{ n = 'error'; e = { $_.error.message } } |
    Format-List
exit 1
