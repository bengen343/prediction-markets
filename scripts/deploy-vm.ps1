#Requires -Version 5.1
<#
.SYNOPSIS
  Deploys the prediction-markets collector code to the GCE VM.

.DESCRIPTION
  Syncs code from this repo to the VM, installs/updates Python deps via uv,
  installs the systemd unit, and restarts the service. Idempotent.

.EXAMPLE
  .\deploy-vm.ps1
#>

param(
  [string]$ProjectId = "",
  [string]$VmName = "collector-vm",
  [string]$Zone = "us-west1-a"
)

$ErrorActionPreference = "Continue"

if (-not $ProjectId) {
  $ProjectId = & gcloud config get-value project 2>$null
  if (-not $ProjectId -or $ProjectId -eq "(unset)") {
    throw "No project set. Pass -ProjectId or run 'gcloud config set project <id>'."
  }
}

$RepoRoot = Split-Path -Parent $PSScriptRoot
$Staging = "/tmp/collector-staging"

function Invoke-Checked {
  param([scriptblock]$Block, [string]$What)
  & $Block
  if ($LASTEXITCODE -ne 0) { throw "Failed: $What (exit $LASTEXITCODE)" }
}

Write-Host ""
Write-Host "=== Deploy to $VmName ($ProjectId) ===" -ForegroundColor Cyan

Write-Host "--- Preparing staging directory on VM ---" -ForegroundColor Yellow
Invoke-Checked {
  gcloud compute ssh $VmName --zone $Zone --project $ProjectId `
    --command "rm -rf $Staging; mkdir -p $Staging"
} "prepare staging"

Write-Host "--- Building payload ---" -ForegroundColor Yellow
$payload = Join-Path $env:TEMP "collector-payload-$PID.tgz"
Invoke-Checked {
  & tar -czf $payload -C $RepoRoot src pyproject.toml systemd -C $PSScriptRoot install-on-vm.sh
} "build payload"

Write-Host "--- Uploading payload to VM ---" -ForegroundColor Yellow
# Stream via `gcloud compute ssh` stdin instead of `gcloud compute scp`: gcloud's Windows scp binds to whichever pscp/scp is on PATH, which has been brittle across machines.
$psi = New-Object System.Diagnostics.ProcessStartInfo
$psi.FileName = "gcloud"
$psi.Arguments = "compute ssh `"$VmName`" --zone `"$Zone`" --project `"$ProjectId`" --command `"tar -xzf - -C $Staging`""
$psi.UseShellExecute = $false
$psi.RedirectStandardInput = $true
try {
  $proc = [System.Diagnostics.Process]::Start($psi)
  $bytes = [System.IO.File]::ReadAllBytes($payload)
  $proc.StandardInput.BaseStream.Write($bytes, 0, $bytes.Length)
  $proc.StandardInput.Close()
  $proc.WaitForExit()
  if ($proc.ExitCode -ne 0) { throw "Failed: upload payload (exit $($proc.ExitCode))" }
} finally {
  Remove-Item $payload -ErrorAction SilentlyContinue
}

Write-Host "--- Running install on VM ---" -ForegroundColor Yellow
Invoke-Checked {
  gcloud compute ssh $VmName --zone $Zone --project $ProjectId `
    --command "sudo bash $Staging/install-on-vm.sh $Staging"
} "install script"

Write-Host ""
Write-Host "=== Deploy complete ===" -ForegroundColor Green
Write-Host "Tail service logs:"
Write-Host "  gcloud compute ssh $VmName --zone $Zone --project $ProjectId --command 'journalctl -u kalshi-collector -f'"
Write-Host "Check recent trades in BQ:"
Write-Host "  bq query --use_legacy_sql=false ""SELECT COUNT(*), MAX(ingested_at) FROM prediction_markets.trades WHERE source='kalshi'"""
