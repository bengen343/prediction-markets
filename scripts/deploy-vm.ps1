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
# sudo rm because previous deploys from a different machine may have left files
# owned by a different OS Login uid (e.g. uid 1000 from a 'ben' user vs the
# current 'ben_amaroqanalytics_com' uid) that this user can't delete otherwise.
Invoke-Checked {
  gcloud compute ssh $VmName --zone $Zone --project $ProjectId `
    --command "sudo rm -rf $Staging && mkdir -p $Staging"
} "prepare staging"

Write-Host "--- Building payload ---" -ForegroundColor Yellow
$payload = Join-Path $env:TEMP "collector-payload-$PID.tgz"
Invoke-Checked {
  & tar -czf $payload -C $RepoRoot src pyproject.toml systemd -C $PSScriptRoot install-on-vm.sh
} "build payload"

Write-Host "--- Uploading payload to VM ---" -ForegroundColor Yellow
# Use `gcloud compute scp` to land the tarball, then ssh to extract.
# We previously tried streaming the tarball over `gcloud compute ssh` stdin via Process.Start, but on Windows the only way to launch `gcloud.cmd` from .NET is through cmd.exe, and cmd.exe corrupts binary stdin (text-mode handle → \r\n / \x1A translation breaks the gzip stream).
$remotePayload = "/tmp/collector-payload.tgz"
try {
  Invoke-Checked {
    gcloud compute scp --zone $Zone --project $ProjectId $payload "${VmName}:${remotePayload}"
  } "upload payload"
  Invoke-Checked {
    gcloud compute ssh $VmName --zone $Zone --project $ProjectId `
      --command "tar -xzf $remotePayload -C $Staging && rm -f $remotePayload"
  } "extract payload"
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
