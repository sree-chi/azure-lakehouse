#Requires -Version 7.2
<#
.SYNOPSIS
  Checks both EPA source URLs from your laptop BEFORE deploying, and shows the
  first bytes of each so you can confirm the columns the notebooks expect.

.EXAMPLE
  ./scripts/Test-Sources.ps1 -ReportingYear 2024
#>
[CmdletBinding()]
param(
    [string] $ReportingYear = '2024',
    [string] $ReleasesUrl = '',
    [string] $FacilitiesUrl = 'https://data.epa.gov/dmapservice/tri.tri_facility/1:3/JSON'
)

$ErrorActionPreference = 'Stop'
if (-not $ReleasesUrl) {
    $ReleasesUrl = "https://data.epa.gov/efservice/downloads/tri/mv_tri_basic_download/${ReportingYear}_US/csv"
}

$client = [System.Net.Http.HttpClient]::new()
$client.Timeout = [TimeSpan]::FromMinutes(10)

function Read-Head([string] $url, [int] $chars) {
    $resp = $client.GetAsync($url, [System.Net.Http.HttpCompletionOption]::ResponseHeadersRead).GetAwaiter().GetResult()
    $code = [int]$resp.StatusCode
    $type = $resp.Content.Headers.ContentType
    $len = $resp.Content.Headers.ContentLength
    $reader = [System.IO.StreamReader]::new($resp.Content.ReadAsStreamAsync().GetAwaiter().GetResult())
    $buf = [char[]]::new($chars)
    $n = $reader.Read($buf, 0, $chars)
    $reader.Dispose(); $resp.Dispose()
    [pscustomobject]@{ Status = $code; ContentType = "$type"; Length = $len; Head = -join $buf[0..([Math]::Max($n - 1, 0))] }
}

Write-Host "Releases CSV : $ReleasesUrl" -ForegroundColor Cyan
$r = Read-Head $ReleasesUrl 4000
$r | Select-Object Status, ContentType, Length | Format-List
$header = ($r.Head -split "`r?`n")[0]
$cols = $header -split ','
Write-Host "Columns found: $($cols.Count)"
foreach ($needle in 'YEAR', 'TRIFD', 'FACILITY NAME', 'CHEMICAL', 'UNIT OF MEASURE', 'TOTAL RELEASES') {
    $hit = $cols | Where-Object { $_ -match [regex]::Escape($needle) } | Select-Object -First 1
    $mark = if ($hit) { 'OK ' } else { 'MISSING' }
    Write-Host ("  {0,-8} {1,-16} -> {2}" -f $mark, $needle, $hit)
}

Write-Host "`nFacilities JSON: $FacilitiesUrl" -ForegroundColor Cyan
$f = Read-Head $FacilitiesUrl 1500
$f | Select-Object Status, ContentType | Format-List
Write-Host $f.Head

Write-Host @"

If both returned 200 and the columns show OK, the defaults in the ADF pipeline will work.
If not, find the working URL (EPA TRI Basic Data Files page -> browser DevTools -> Network ->
click Download) and pass it to the pipeline parameters releasesPathPrefix / releasesPathSuffix /
facilitiesRelativeUrl (ADF Studio > pipeline > Parameters), no redeploy needed.
"@
