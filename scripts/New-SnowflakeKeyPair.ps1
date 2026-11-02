#Requires -Version 7.2
<#
.SYNOPSIS
  Generates the RSA key pair for the Snowflake service user (key-pair auth).
  Private key -> .secrets/snowflake_rsa_key.p8 (git-ignored; Deploy-All.ps1 moves it into Key Vault)
  Public key  -> printed as the ALTER USER statement to run in Snowflake.

  Needs openssl: built into macOS/Linux; on Windows it ships with Git for Windows
  (C:\Program Files\Git\usr\bin\openssl.exe).
#>
[CmdletBinding()]
param(
    [string] $OutDir = (Join-Path (Split-Path -Parent $PSScriptRoot) '.secrets'),
    [string] $SnowflakeUser = 'TRI_DATABRICKS_SVC'
)

$ErrorActionPreference = 'Stop'
$openssl = (Get-Command openssl -ErrorAction SilentlyContinue)?.Source
if (-not $openssl -and $IsWindows) {
    $candidate = 'C:\Program Files\Git\usr\bin\openssl.exe'
    if (Test-Path $candidate) { $openssl = $candidate }
}
if (-not $openssl) { throw 'openssl not found. Install Git for Windows (Windows) or use the system openssl (macOS/Linux).' }

New-Item -ItemType Directory -Force -Path $OutDir | Out-Null
$rsa = Join-Path $OutDir 'snowflake_rsa_key.pem'
$p8 = Join-Path $OutDir 'snowflake_rsa_key.p8'
$pub = Join-Path $OutDir 'snowflake_rsa_key.pub'

& $openssl genrsa -out $rsa 2048
& $openssl pkcs8 -topk8 -inform PEM -in $rsa -outform PEM -nocrypt -out $p8
& $openssl rsa -in $rsa -pubout -out $pub
Remove-Item $rsa

$pubBody = (Get-Content $pub | Where-Object { $_ -notmatch '-----' }) -join ''

Write-Host "`nPrivate key (keep secret): $p8" -ForegroundColor Yellow
Write-Host "Run this in Snowflake (or paste the key into snowflake/setup.sql):`n" -ForegroundColor Cyan
Write-Host "ALTER USER $SnowflakeUser SET RSA_PUBLIC_KEY = '$pubBody';"
