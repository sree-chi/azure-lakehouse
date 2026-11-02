#Requires -Version 7.2
<#
.SYNOPSIS
  Tears everything down in reverse order so nothing keeps billing.
  1. Databricks stage (removes the Unity Catalog catalog/credential, which live outside the RG)
  2. Platform stage (deletes the resource group, including the Bicep-deployed SQL DB and ADF pipeline)
#>
[CmdletBinding(SupportsShouldProcess)]
param()

$ErrorActionPreference = 'Stop'
$repo = Split-Path -Parent $PSScriptRoot

foreach ($stage in 'databricks', 'platform') {
    $dir = Join-Path $repo "infra/terraform/$stage"
    if (-not (Test-Path (Join-Path $dir 'terraform.tfstate'))) {
        Write-Host "No state for '$stage' stage; skipping."
        continue
    }
    if ($PSCmdlet.ShouldProcess($stage, 'terraform destroy')) {
        Write-Host "`n==> terraform destroy ($stage)" -ForegroundColor Cyan
        terraform -chdir="$dir" destroy -input=false -auto-approve
        if ($LASTEXITCODE -ne 0) { throw "destroy failed for $stage" }
    }
}

Write-Host @"

Azure side removed. If you used Snowflake, run snowflake/teardown.sql in a worksheet
(the free trial also ends on its own after 30 days).
"@ -ForegroundColor Green
