#Requires -Version 7.2
<#
.SYNOPSIS
  Deploys the whole TRI lakehouse: Terraform platform -> Key Vault secrets ->
  Terraform Databricks -> Bicep (SQL database + ADF pipeline).

.EXAMPLE
  ./scripts/Deploy-All.ps1 -SubscriptionId <guid> -Location centralus -AlertEmail schin68@illinois.edu

.EXAMPLE
  # With the Snowflake load turned on (run snowflake/setup.sql first)
  ./scripts/Deploy-All.ps1 -SubscriptionId <guid> -EnableSnowflake `
      -SnowflakeAccountUrl myorg-myacct.snowflakecomputing.com `
      -SnowflakePrivateKeyPath ./.secrets/snowflake_rsa_key.p8
#>
[CmdletBinding()]
param(
    [Parameter(Mandatory)] [string] $SubscriptionId,
    [string] $Location = 'centralus',
    [ValidatePattern('^[a-z0-9]{2,6}$')] [string] $Prefix = 'trilh',
    [string] $ReportingYear = '2024',
    [string] $AlertEmail = '',
    [ValidateSet('trial', 'premium')] [string] $DatabricksSku = 'trial',
    [string] $NodeType = 'Standard_DS3_v2',
    [switch] $NoSqlFreeOffer,
    [switch] $EnableSnowflake,
    [string] $SnowflakeAccountUrl = '',
    [string] $SnowflakeUser = 'TRI_DATABRICKS_SVC',
    [string] $SnowflakePrivateKeyPath = ''
)

$ErrorActionPreference = 'Stop'
$repo = Split-Path -Parent $PSScriptRoot
$tfPlatform = Join-Path $repo 'infra/terraform/platform'
$tfDatabricks = Join-Path $repo 'infra/terraform/databricks'
$bicepMain = Join-Path $repo 'infra/bicep/main.bicep'

function Step([string] $msg) { Write-Host "`n==> $msg" -ForegroundColor Cyan }

function Invoke-Native {
    param([Parameter(Mandatory)] [string] $What, [Parameter(Mandatory)] [scriptblock] $Command)
    & $Command
    if ($LASTEXITCODE -ne 0) { throw "$What failed (exit code $LASTEXITCODE)" }
}

# --------------------------------------------------------------------------
Step 'Checking tools'
foreach ($tool in 'az', 'terraform') {
    if (-not (Get-Command $tool -ErrorAction SilentlyContinue)) {
        throw "$tool is not installed or not on PATH. See README > Prerequisites."
    }
}
Invoke-Native 'az bicep install/upgrade' { az bicep upgrade --only-show-errors 2>$null; if ($LASTEXITCODE -ne 0) { az bicep install --only-show-errors } }

if ($EnableSnowflake) {
    if (-not $SnowflakeAccountUrl) { throw '-SnowflakeAccountUrl is required with -EnableSnowflake' }
    if (-not (Test-Path $SnowflakePrivateKeyPath)) { throw "Private key not found: $SnowflakePrivateKeyPath (run scripts/New-SnowflakeKeyPair.ps1)" }
}

# --------------------------------------------------------------------------
Step "Selecting subscription $SubscriptionId"
Invoke-Native 'az account set' { az account set --subscription $SubscriptionId }
$me = az account show --query user.name -o tsv
Write-Host "Signed in as $me"

$clientIp = ''
try { $clientIp = (Invoke-RestMethod -Uri 'https://api.ipify.org' -TimeoutSec 10).Trim() } catch { }
if ($clientIp) { Write-Host "Your public IP (allowed through the SQL firewall): $clientIp" }

# --------------------------------------------------------------------------
Step 'Terraform: platform (RG, ADLS, Key Vault, ADF, Databricks, SQL server)'
$platformVars = [ordered]@{
    subscription_id = $SubscriptionId
    location        = $Location
    prefix          = $Prefix
    databricks_sku  = $DatabricksSku
    client_ip       = $clientIp
    alert_email     = $AlertEmail
}
$platformVars | ConvertTo-Json | Set-Content (Join-Path $tfPlatform 'deploy.auto.tfvars.json') -Encoding utf8

Invoke-Native 'terraform init (platform)' { terraform -chdir="$tfPlatform" init -input=false -upgrade }
Invoke-Native 'terraform apply (platform)' { terraform -chdir="$tfPlatform" apply -input=false -auto-approve }
$p = terraform -chdir="$tfPlatform" output -json | ConvertFrom-Json

# --------------------------------------------------------------------------
if ($EnableSnowflake) {
    Step 'Storing Snowflake credentials in Key Vault'
    Invoke-Native 'kv secret snowflake-account-url' { az keyvault secret set --vault-name $p.key_vault_name.value -n snowflake-account-url --value $SnowflakeAccountUrl -o none }
    Invoke-Native 'kv secret snowflake-user' { az keyvault secret set --vault-name $p.key_vault_name.value -n snowflake-user --value $SnowflakeUser -o none }
    Invoke-Native 'kv secret snowflake-private-key' { az keyvault secret set --vault-name $p.key_vault_name.value -n snowflake-private-key --file $SnowflakePrivateKeyPath -o none }
    Write-Host 'Stored. You can now delete the local private key file.' -ForegroundColor Yellow
}

# --------------------------------------------------------------------------
Step 'Terraform: Databricks (Unity Catalog, secret scope, notebooks, job, ADF permissions)'
$dbxVars = [ordered]@{
    workspace_url         = $p.databricks_workspace_url.value
    workspace_resource_id = $p.databricks_workspace_resource_id.value
    access_connector_id   = $p.access_connector_id.value
    storage_account_name  = $p.storage_account_name.value
    key_vault_id          = $p.key_vault_id.value
    key_vault_uri         = $p.key_vault_uri.value
    adf_identity_app_id   = $p.data_factory_identity_app_id.value
    sql_server_fqdn       = $p.sql_server_fqdn.value
    sql_database_name     = $p.sql_database_name.value
    node_type_id          = $NodeType
    reporting_year        = $ReportingYear
    enable_snowflake      = [bool]$EnableSnowflake
}
$dbxVars | ConvertTo-Json | Set-Content (Join-Path $tfDatabricks 'deploy.auto.tfvars.json') -Encoding utf8

Invoke-Native 'terraform init (databricks)' { terraform -chdir="$tfDatabricks" init -input=false -upgrade }
Invoke-Native 'terraform apply (databricks)' { terraform -chdir="$tfDatabricks" apply -input=false -auto-approve }
$d = terraform -chdir="$tfDatabricks" output -json | ConvertFrom-Json

# --------------------------------------------------------------------------
Step 'Bicep: Azure SQL database (free offer) + ADF pipeline'
$useFree = if ($NoSqlFreeOffer) { 'false' } else { 'true' }
Invoke-Native 'az deployment group create' {
    az deployment group create `
        --resource-group $p.resource_group_name.value `
        --name tri-pipeline-layer `
        --template-file $bicepMain `
        --parameters `
            factoryName=$($p.data_factory_name.value) `
            storageAccountName=$($p.storage_account_name.value) `
            databricksWorkspaceUrl=$($p.databricks_workspace_url.value) `
            databricksJobId=$($d.job_id.value) `
            sqlServerName=$($p.sql_server_name.value) `
            sqlDatabaseName=$($p.sql_database_name.value) `
            useSqlFreeLimit=$useFree `
            reportingYear=$ReportingYear `
        --only-show-errors -o none
}

# --------------------------------------------------------------------------
Step 'Done'
[pscustomobject]@{
    ResourceGroup  = $p.resource_group_name.value
    DataFactory    = $p.data_factory_name.value
    Databricks     = $p.databricks_workspace_url.value
    DatabricksJob  = $d.job_url.value
    SqlServer      = $p.sql_server_fqdn.value
    SqlDatabase    = $p.sql_database_name.value
    KeyVault       = $p.key_vault_name.value
    Snowflake      = [bool]$EnableSnowflake
} | Format-List

Write-Host 'Next: ./scripts/Start-Pipeline.ps1   (runs ADF end to end and waits for it)' -ForegroundColor Green
