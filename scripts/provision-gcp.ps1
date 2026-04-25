#Requires -Version 5.1
<#
.SYNOPSIS
  Provisions GCP resources for the prediction-markets project.

.DESCRIPTION
  Creates the GCP project, enables APIs, creates the service account, GCS config
  bucket, BigQuery dataset+tables, Secret Manager entries, and the e2-micro VM.
  Idempotent: safe to re-run.

.EXAMPLE
  .\provision-gcp.ps1 -BillingAccountId "0X0X0X-0X0X0X-0X0X0X"

.EXAMPLE
  .\provision-gcp.ps1 -BillingAccountId "..." `
    -KalshiPrivateKeyFile "$HOME\secrets\kalshi_private.txt" `
    -KalshiApiKeyId "abcd-1234" `
    -DiscordWebhookUrl "https://discord.com/api/webhooks/..."
#>
# TODO: Convert the GCP infrastructure setup to Terraform.


param(
  [Parameter(Mandatory = $true)]
  [string]$BillingAccountId,

  [string]$ProjectId = "",
  [string]$Region = "us-west1",
  [string]$Zone = "us-west1-a",
  [string]$VmName = "collector-vm",
  [string]$ServiceAccountName = "collector-sa",
  [string]$DatasetName = "prediction_markets",

  [string]$KalshiPrivateKeyFile = "",
  [string]$KalshiApiKeyId = "",
  [string]$DiscordWebhookUrl = ""
)

# NOTE: Using Continue (not Stop). PS 5.1 wraps native-command stderr as
# NativeCommandError records; under Stop, even benign stderr from gcloud halts
# the script. We rely on $LASTEXITCODE checks instead.
$ErrorActionPreference = "Continue"

# --- Preflight ---

if (-not (Get-Command gcloud -ErrorAction SilentlyContinue)) {
  throw "gcloud CLI not found. Install from https://cloud.google.com/sdk/docs/install"
}
if (-not (Get-Command bq -ErrorAction SilentlyContinue)) {
  throw "bq CLI not found. Run 'gcloud components install bq'."
}

$authAccount = & gcloud config get-value account 2>$null
if (-not $authAccount -or $authAccount -eq "(unset)") {
  throw "Not authenticated. Run 'gcloud auth login' first."
}

# Resolve project ID: explicit param wins; else use current gcloud-active
# project; else generate a new one. Defaulting to the active project prevents
# accidentally spinning up a duplicate setup when re-running the script.
if (-not $ProjectId) {
  $currentProject = & gcloud config get-value project 2>$null
  if ($currentProject -and $currentProject -ne "(unset)") {
    $ProjectId = $currentProject
    Write-Host "Using gcloud-active project: $ProjectId"
  } else {
    $suffix = [Guid]::NewGuid().ToString("N").Substring(0, 6).ToLower()
    $ProjectId = "prediction-markets-$suffix"
    Write-Host "No gcloud-active project; generated new ID: $ProjectId"
  }
}

$ConfigBucket = "$ProjectId-config"
$ServiceAccountEmail = "$ServiceAccountName@$ProjectId.iam.gserviceaccount.com"

Write-Host ""
Write-Host "=== prediction-markets GCP provisioning ===" -ForegroundColor Cyan
Write-Host "Authenticated as:  $authAccount"
Write-Host "Project ID:        $ProjectId"
Write-Host "Region / Zone:     $Region / $Zone"
Write-Host "Service Account:   $ServiceAccountEmail"
Write-Host "Config Bucket:     gs://$ConfigBucket"
Write-Host "BQ Dataset:        ${ProjectId}:${DatasetName}"
Write-Host ""

# --- Helpers ---

function Test-GcloudResource {
  param([string[]]$DescribeArgs)
  & gcloud @DescribeArgs --quiet 2>$null | Out-Null
  return ($LASTEXITCODE -eq 0)
}

function Test-NativeSuccess {
  param([scriptblock]$Block)
  & $Block 2>$null | Out-Null
  return ($LASTEXITCODE -eq 0)
}

function Invoke-Checked {
  param([scriptblock]$Block, [string]$What)
  & $Block
  if ($LASTEXITCODE -ne 0) { throw "Failed: $What (exit $LASTEXITCODE)" }
}

# --- 1. Project & billing ---

Write-Host "--- Project & billing ---" -ForegroundColor Yellow
if (-not (Test-GcloudResource @("projects", "describe", $ProjectId))) {
  Write-Host "Creating project $ProjectId..."
  Invoke-Checked { gcloud projects create $ProjectId --name "prediction-markets" --quiet } "create project"
} else {
  Write-Host "Project $ProjectId already exists, skipping create."
}

Invoke-Checked { gcloud config set project $ProjectId --quiet } "set default project"

Write-Host "Linking billing account $BillingAccountId..."
Invoke-Checked { gcloud billing projects link $ProjectId --billing-account $BillingAccountId --quiet } "link billing"

# --- 2. Enable APIs ---

Write-Host "--- Enable APIs ---" -ForegroundColor Yellow
$apis = @(
  "compute.googleapis.com",
  "bigquery.googleapis.com",
  "bigquerydatatransfer.googleapis.com",
  "storage.googleapis.com",
  "secretmanager.googleapis.com",
  "iam.googleapis.com",
  "logging.googleapis.com",
  "monitoring.googleapis.com"
)
Invoke-Checked { gcloud services enable @apis --quiet } "enable APIs"

# --- 3. Service account & IAM ---

Write-Host "--- Service account & IAM ---" -ForegroundColor Yellow
if (-not (Test-GcloudResource @("iam", "service-accounts", "describe", $ServiceAccountEmail))) {
  Invoke-Checked {
    gcloud iam service-accounts create $ServiceAccountName `
      --display-name "Prediction Markets Collector" --quiet
  } "create service account"
} else {
  Write-Host "Service account already exists, skipping create."
}

# Roles are additive; re-binding an existing role is a no-op.
$roles = @(
  "roles/bigquery.dataEditor",
  "roles/bigquery.jobUser",
  "roles/secretmanager.secretAccessor",
  "roles/storage.objectUser",
  "roles/logging.logWriter",
  "roles/monitoring.metricWriter"
)
foreach ($role in $roles) {
  Write-Host "  Binding $role..."
  Invoke-Checked {
    gcloud projects add-iam-policy-binding $ProjectId `
      --member "serviceAccount:$ServiceAccountEmail" `
      --role $role --condition=None --quiet | Out-Null
  } "bind $role"
}

# --- 4. GCS config bucket ---

Write-Host "--- GCS config bucket ---" -ForegroundColor Yellow
if (-not (Test-GcloudResource @("storage", "buckets", "describe", "gs://$ConfigBucket"))) {
  Invoke-Checked {
    gcloud storage buckets create "gs://$ConfigBucket" `
      --location $Region --uniform-bucket-level-access --quiet
  } "create GCS bucket"
} else {
  Write-Host "Bucket gs://$ConfigBucket already exists, skipping create."
}

# Seed initial markets.yaml from the example if the bucket doesn't have one yet.
$marketsExample = Join-Path $PSScriptRoot "..\config\markets.example.yaml"
if (Test-Path $marketsExample) {
  $marketsExists = Test-NativeSuccess { gcloud storage ls "gs://$ConfigBucket/markets.yaml" --quiet }
  if (-not $marketsExists) {
    Write-Host "Seeding markets.yaml from example..."
    Invoke-Checked {
      gcloud storage cp $marketsExample "gs://$ConfigBucket/markets.yaml" --quiet
    } "upload markets.yaml"
  } else {
    Write-Host "markets.yaml already present in bucket, leaving untouched."
  }
}

# --- 5. BigQuery dataset & tables ---

Write-Host "--- BigQuery ---" -ForegroundColor Yellow
$dsExists = Test-NativeSuccess { bq --project_id=$ProjectId show --format=none $DatasetName }
if (-not $dsExists) {
  Write-Host "Creating dataset $DatasetName..."
  Invoke-Checked {
    bq --project_id=$ProjectId --location=$Region mk --dataset `
      --description "prediction markets trade data" $DatasetName
  } "create dataset"
} else {
  Write-Host "Dataset $DatasetName already exists, skipping create."
}

$sqlDir = Join-Path $PSScriptRoot "..\sql"
$ddlFiles = @(
  "trades_table.sql",
  "alerts_table.sql",
  "markets_table.sql",
  "markets_staging_table.sql",
  "polymarket_markets_table.sql",
  "polymarket_markets_staging_table.sql"
)
foreach ($ddl in $ddlFiles) {
  $path = Join-Path $sqlDir $ddl
  Write-Host "Applying $ddl..."
  # Flatten SQL onto one line: Windows argv handling of multi-line strings
  # passed to native exes is unreliable. SQL is whitespace-agnostic.
  $sqlText = (Get-Content $path -Raw) -replace "\s+", " "
  Invoke-Checked {
    & bq --project_id=$ProjectId --location=$Region query --use_legacy_sql=false --quiet $sqlText
  } "apply $ddl"
}

# --- 6. Secret Manager ---

Write-Host "--- Secret Manager ---" -ForegroundColor Yellow

function Ensure-Secret {
  param(
    [string]$Name,
    [string]$ValueFromFile = "",
    [string]$ValueInline = ""
  )
  if (-not (Test-GcloudResource @("secrets", "describe", $Name))) {
    Invoke-Checked {
      gcloud secrets create $Name --replication-policy automatic --quiet
    } "create secret $Name"
  }
  if ($ValueFromFile) {
    Write-Host "  Adding version to $Name from file..."
    Invoke-Checked {
      gcloud secrets versions add $Name --data-file $ValueFromFile --quiet | Out-Null
    } "add version for $Name"
  } elseif ($ValueInline) {
    # Write to a temp file with exact bytes - PS piping to native commands
    # appends a trailing newline (CRLF on Windows), which would land in the
    # secret and break consumers that don't strip whitespace (e.g., bash $()).
    Write-Host "  Adding version to $Name..."
    $tmp = [System.IO.Path]::GetTempFileName()
    try {
      [System.IO.File]::WriteAllBytes($tmp, [System.Text.Encoding]::UTF8.GetBytes($ValueInline))
      Invoke-Checked {
        gcloud secrets versions add $Name --data-file=$tmp --quiet | Out-Null
      } "add version for $Name"
    } finally {
      Remove-Item -Force $tmp -ErrorAction SilentlyContinue
    }
  } else {
    Write-Host "  $Name created (no value yet - add one with 'gcloud secrets versions add')."
  }
}

Ensure-Secret -Name "kalshi-private-key" -ValueFromFile $KalshiPrivateKeyFile
Ensure-Secret -Name "kalshi-api-key-id" -ValueInline $KalshiApiKeyId
Ensure-Secret -Name "discord-webhook-url" -ValueInline $DiscordWebhookUrl

# --- 7. GCE VM ---

Write-Host "--- GCE VM ---" -ForegroundColor Yellow
$startupScript = Join-Path $PSScriptRoot "vm-startup.sh"

if (-not (Test-GcloudResource @("compute", "instances", "describe", $VmName, "--zone", $Zone))) {
  Write-Host "Creating VM $VmName..."
  Invoke-Checked {
    gcloud compute instances create $VmName `
      --zone $Zone `
      --machine-type e2-micro `
      --image-family debian-12 `
      --image-project debian-cloud `
      --boot-disk-size 30GB `
      --boot-disk-type pd-standard `
      --service-account $ServiceAccountEmail `
      --scopes cloud-platform `
      --metadata-from-file "startup-script=$startupScript" `
      --tags collector `
      --quiet
  } "create VM"
} else {
  Write-Host "VM $VmName already exists, skipping create."
}

# --- Summary ---

Write-Host ""
Write-Host "=== Done ===" -ForegroundColor Green
Write-Host "Project:       $ProjectId"
Write-Host "VM:            $VmName in $Zone"
Write-Host "Config bucket: gs://$ConfigBucket/markets.yaml"
Write-Host "BQ dataset:    ${ProjectId}:${DatasetName}"
Write-Host ""
Write-Host "SSH into the VM:"
Write-Host "  gcloud compute ssh $VmName --zone $Zone --project $ProjectId"
Write-Host ""

$missing = @()
if (-not $KalshiPrivateKeyFile) { $missing += "kalshi-private-key    (gcloud secrets versions add kalshi-private-key --data-file=PATH)" }
if (-not $KalshiApiKeyId)       { $missing += "kalshi-api-key-id     (echo VALUE | gcloud secrets versions add kalshi-api-key-id --data-file=-)" }
if (-not $DiscordWebhookUrl)    { $missing += "discord-webhook-url   (echo VALUE | gcloud secrets versions add discord-webhook-url --data-file=-)" }
if ($missing.Count -gt 0) {
  Write-Host "Secrets still needing values:" -ForegroundColor Yellow
  foreach ($m in $missing) { Write-Host "  - $m" }
}
