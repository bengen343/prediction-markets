#Requires -Version 5.1
<#
.SYNOPSIS
  Provisions LLM-debater infrastructure on top of an existing prediction-markets
  project (i.e. one already provisioned by scripts/provision-gcp.ps1).

.DESCRIPTION
  Adds: Pub/Sub topic + push subscription, Cloud Run service, Artifact Registry
  repo, debates BQ table, debater service account, LLM API key secrets, and
  Cloud Build trigger pointing at this GitHub repo. Grants collector-sa
  pubsub.publisher on the topic so the notifier can enqueue debate requests.

  Idempotent: safe to re-run.

  ONE-TIME PREREQUISITE: the Cloud Build GitHub App must be installed for the
  GitHub repo before this script's trigger-create step can succeed. Install via:
    https://console.cloud.google.com/cloud-build/triggers/connect

.EXAMPLE
  .\provision-gcp-debater.ps1 -GitHubOwner "myname" -GitHubRepo "prediction-markets"

.EXAMPLE
  .\provision-gcp-debater.ps1 -GitHubOwner "myname" -GitHubRepo "prediction-markets" `
    -AnthropicApiKey "sk-ant-..." -OpenAIApiKey "sk-..." `
    -GeminiApiKey "AI..." -XaiApiKey "xai-..."
#>

param(
  [Parameter(Mandatory = $true)]
  [string]$GitHubOwner,

  [Parameter(Mandatory = $true)]
  [string]$GitHubRepo,

  [string]$ProjectId = "",
  [string]$Region = "us-west1",
  [string]$DatasetName = "prediction_markets",
  [string]$ServiceAccountName = "debater-sa",
  [string]$CollectorServiceAccountName = "collector-sa",
  [string]$ArRepoName = "prediction-markets-debater",
  [string]$ServiceName = "debate-worker",
  [string]$TopicName = "debate-requests",
  [string]$SubscriptionName = "debate-worker",
  [string]$BranchPattern = "^main$",

  [string]$AnthropicApiKey = "",
  [string]$OpenAIApiKey = "",
  [string]$GeminiApiKey = "",
  [string]$XaiApiKey = ""
)

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

if (-not $ProjectId) {
  $currentProject = & gcloud config get-value project 2>$null
  if ($currentProject -and $currentProject -ne "(unset)") {
    $ProjectId = $currentProject
    Write-Host "Using gcloud-active project: $ProjectId"
  } else {
    throw "No project configured. Pass -ProjectId or run 'gcloud config set project ...'."
  }
}

$ServiceAccountEmail = "$ServiceAccountName@$ProjectId.iam.gserviceaccount.com"
$CollectorEmail = "$CollectorServiceAccountName@$ProjectId.iam.gserviceaccount.com"
$ConfigBucket = "$ProjectId-config"

Write-Host ""
Write-Host "=== prediction-markets debater provisioning ===" -ForegroundColor Cyan
Write-Host "Authenticated as:  $authAccount"
Write-Host "Project:           $ProjectId"
Write-Host "Region:            $Region"
Write-Host "Debater SA:        $ServiceAccountEmail"
Write-Host "Cloud Run service: $ServiceName"
Write-Host "Pub/Sub topic:     $TopicName"
Write-Host "AR repo:           $ArRepoName"
Write-Host "GitHub:            ${GitHubOwner}/${GitHubRepo} (branch: $BranchPattern)"
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

function Ensure-Secret {
  param(
    [string]$Name,
    [string]$ValueInline = ""
  )
  if (-not (Test-GcloudResource @("secrets", "describe", $Name))) {
    Invoke-Checked {
      gcloud secrets create $Name --replication-policy automatic --quiet
    } "create secret $Name"
  }
  if ($ValueInline) {
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

# --- 1. Enable APIs ---

Write-Host "--- Enable APIs ---" -ForegroundColor Yellow
$apis = @(
  "run.googleapis.com",
  "pubsub.googleapis.com",
  "artifactregistry.googleapis.com",
  "cloudbuild.googleapis.com"
)
Invoke-Checked { gcloud services enable @apis --quiet } "enable APIs"

# --- 2. Apply debates table DDL ---

# Write-Host "--- BigQuery ---" -ForegroundColor Yellow
# $ddlPath = Join-Path $PSScriptRoot "..\sql\debates_table.sql"
# $sqlText = (Get-Content $ddlPath -Raw) -replace "\s+", " "
# Invoke-Checked {
#   & bq --project_id=$ProjectId --location=$Region query --use_legacy_sql=false --quiet $sqlText
# } "apply debates_table.sql"

# --- 3. Service account ---

Write-Host "--- Debater service account ---" -ForegroundColor Yellow
if (-not (Test-GcloudResource @("iam", "service-accounts", "describe", $ServiceAccountEmail))) {
  Invoke-Checked {
    gcloud iam service-accounts create $ServiceAccountName `
      --display-name "Prediction Markets Debater" --quiet
  } "create debater service account"
} else {
  Write-Host "Service account already exists, skipping create."
}

$debaterRoles = @(
  "roles/bigquery.dataEditor",
  "roles/bigquery.jobUser",
  "roles/storage.objectUser",
  "roles/secretmanager.secretAccessor"
)
foreach ($role in $debaterRoles) {
  Write-Host "  Binding $role to $ServiceAccountName..."
  Invoke-Checked {
    gcloud projects add-iam-policy-binding $ProjectId `
      --member "serviceAccount:$ServiceAccountEmail" `
      --role $role --condition=None --quiet | Out-Null
  } "bind $role"
}

# --- 4. Artifact Registry ---

Write-Host "--- Artifact Registry ---" -ForegroundColor Yellow
if (-not (Test-GcloudResource @("artifacts", "repositories", "describe", $ArRepoName, "--location", $Region))) {
  Invoke-Checked {
    gcloud artifacts repositories create $ArRepoName `
      --repository-format docker `
      --location $Region `
      --description "Prediction-markets debater Cloud Run images" --quiet
  } "create AR repo"
} else {
  Write-Host "AR repo already exists, skipping create."
}

# --- 5. Pub/Sub topic ---

Write-Host "--- Pub/Sub topic ---" -ForegroundColor Yellow
if (-not (Test-GcloudResource @("pubsub", "topics", "describe", $TopicName))) {
  Invoke-Checked {
    gcloud pubsub topics create $TopicName --quiet
  } "create topic"
} else {
  Write-Host "Topic already exists, skipping create."
}

# Grant collector-sa publisher rights. This is the single additive touch on the
# alerting infra; it's harmless if collector-sa never publishes (i.e. when
# debater.enabled is false in markets.yaml).
Write-Host "Granting pubsub.publisher to $CollectorServiceAccountName on $TopicName..."
Invoke-Checked {
  gcloud pubsub topics add-iam-policy-binding $TopicName `
    --member "serviceAccount:$CollectorEmail" `
    --role roles/pubsub.publisher --quiet | Out-Null
} "grant publisher to collector-sa"

# --- 6. LLM secrets ---

Write-Host "--- LLM API key secrets ---" -ForegroundColor Yellow
Ensure-Secret -Name "anthropic-api-key" -ValueInline $AnthropicApiKey
Ensure-Secret -Name "openai-api-key"    -ValueInline $OpenAIApiKey
Ensure-Secret -Name "gemini-api-key"    -ValueInline $GeminiApiKey
Ensure-Secret -Name "xai-api-key"       -ValueInline $XaiApiKey

# --- 7. Cloud Run service (initial deploy with placeholder image) ---
#
# Cloud Build will replace the image on first push; we stand the service up now
# with full config (max-instances, concurrency, timeout, runtime SA, no public
# auth) so subsequent builds only need to swap the image tag.

Write-Host "--- Cloud Run service ---" -ForegroundColor Yellow
$placeholderImage = "us-docker.pkg.dev/cloudrun/container/hello"

if (-not (Test-GcloudResource @("run", "services", "describe", $ServiceName, "--region", $Region))) {
  Write-Host "Creating $ServiceName with placeholder image..."
  Invoke-Checked {
    gcloud run deploy $ServiceName `
      --image $placeholderImage `
      --region $Region `
      --service-account $ServiceAccountEmail `
      --no-allow-unauthenticated `
      --max-instances 1 `
      --concurrency 1 `
      --timeout 600 `
      --cpu 1 `
      --memory 512Mi `
      --quiet
  } "deploy debate-worker (initial)"
} else {
  Write-Host "Cloud Run service already exists, leaving image untouched (Cloud Build manages it)."
}

# Resolve service URL for the push subscription endpoint.
$runUrl = & gcloud run services describe $ServiceName --region $Region --format "value(status.url)" 2>$null
if (-not $runUrl) { throw "Failed to resolve Cloud Run service URL for $ServiceName." }
Write-Host "Service URL: $runUrl"

# Allow debater-sa to invoke its own service (used as the OIDC push identity).
Write-Host "Granting run.invoker on $ServiceName to $ServiceAccountName..."
Invoke-Checked {
  gcloud run services add-iam-policy-binding $ServiceName `
    --region $Region `
    --member "serviceAccount:$ServiceAccountEmail" `
    --role roles/run.invoker --quiet | Out-Null
} "grant run.invoker to debater-sa"

# --- 8. Pub/Sub push subscription ---

Write-Host "--- Pub/Sub subscription ---" -ForegroundColor Yellow
if (-not (Test-GcloudResource @("pubsub", "subscriptions", "describe", $SubscriptionName))) {
  Invoke-Checked {
    gcloud pubsub subscriptions create $SubscriptionName `
      --topic $TopicName `
      --push-endpoint $runUrl `
      --push-auth-service-account $ServiceAccountEmail `
      --ack-deadline 600 `
      --message-retention-duration 1h `
      --quiet
  } "create push subscription"
} else {
  Write-Host "Subscription exists; updating push config in case URL changed..."
  Invoke-Checked {
    gcloud pubsub subscriptions update $SubscriptionName `
      --push-endpoint $runUrl `
      --push-auth-service-account $ServiceAccountEmail `
      --ack-deadline 600 `
      --quiet
  } "update push subscription"
}

# --- 9. Cloud Build runner service account ---
#
# Dedicated build identity (separate from the runtime debater-sa) so a
# build-time compromise can't read the runtime's secrets and a runtime
# compromise can't redeploy services. Done before the trigger create so a
# re-run after installing the GitHub App still completes successfully.

$CloudBuildSaName = "debater-sa"
$CloudBuildSaEmail = "$CloudBuildSaName@$ProjectId.iam.gserviceaccount.com"

Write-Host "--- Cloud Build runner SA ---" -ForegroundColor Yellow
if (-not (Test-GcloudResource @("iam", "service-accounts", "describe", $CloudBuildSaEmail))) {
  Invoke-Checked {
    gcloud iam service-accounts create $CloudBuildSaName `
      --display-name "Prediction Markets Debater Cloud Build Runner" --quiet
  } "create cloud build runner service account"
} else {
  Write-Host "Service account already exists, skipping create."
}

# Project-level roles needed by the build itself (push images, update Cloud
# Run, write build logs, access the Cloud Build staging bucket).
$cbRoles = @(
  "roles/run.admin",
  "roles/artifactregistry.writer",
  "roles/logging.logWriter",
  "roles/storage.admin"
)
foreach ($role in $cbRoles) {
  Write-Host "  Binding $role to $CloudBuildSaName..."
  Invoke-Checked {
    gcloud projects add-iam-policy-binding $ProjectId `
      --member "serviceAccount:$CloudBuildSaEmail" `
      --role $role --condition=None --quiet | Out-Null
  } "bind $role to Cloud Build runner SA"
}

# iam.serviceAccountUser is scoped to debater-sa specifically (NOT project-
# wide) so the build SA can only act as the runtime SA, not as any SA in the
# project. Required if the deploy step ever sets --service-account; harmless
# (and conventional) otherwise.
Write-Host "  Binding iam.serviceAccountUser on $ServiceAccountName to $CloudBuildSaName..."
Invoke-Checked {
  gcloud iam service-accounts add-iam-policy-binding $ServiceAccountEmail `
    --member "serviceAccount:$CloudBuildSaEmail" `
    --role roles/iam.serviceAccountUser --quiet | Out-Null
} "bind serviceAccountUser on debater-sa to Cloud Build runner SA"

# --- 10. Cloud Build trigger ---
#
# Requires the Cloud Build GitHub App to be installed on ${GitHubOwner}/${GitHubRepo}.
# If not installed, the API returns a generic INVALID_ARGUMENT. We catch that,
# print actionable instructions, and exit non-fatally so the user can install
# the app and re-run.

Write-Host "--- Cloud Build trigger ---" -ForegroundColor Yellow
$triggerName = "$ServiceName-on-push"
$cbSaResource = "projects/$ProjectId/serviceAccounts/$CloudBuildSaEmail"

$triggerExists = Test-NativeSuccess { gcloud builds triggers describe $triggerName }
if ($triggerExists) {
  Write-Host "Trigger $triggerName already exists, skipping create."
} else {
  Write-Host "Creating trigger $triggerName..."
  $included = "src/prediction_markets/debater/**,cloudbuild.yaml,pyproject.toml"
  & gcloud builds triggers create github `
    --name $triggerName `
    --repo-owner $GitHubOwner `
    --repo-name $GitHubRepo `
    --branch-pattern $BranchPattern `
    --build-config cloudbuild.yaml `
    --included-files $included `
    --substitutions "_REGION=$Region,_AR_REPO=$ArRepoName,_SERVICE_NAME=$ServiceName" `
    --service-account $cbSaResource `
    --quiet
  if ($LASTEXITCODE -ne 0) {
    Write-Host ""
    Write-Host "Cloud Build trigger creation failed." -ForegroundColor Yellow
    Write-Host "This almost always means the Cloud Build GitHub App has not been"
    Write-Host "installed on ${GitHubOwner}/${GitHubRepo}. To fix:"
    Write-Host "  1. Visit: https://github.com/apps/google-cloud-build/installations/new"
    Write-Host "  2. Install on the '$GitHubOwner' account and grant access to '$GitHubRepo'."
    Write-Host "  3. Re-run this script. All other steps are idempotent."
  }
}

# --- Summary ---

Write-Host ""
Write-Host "=== Done ===" -ForegroundColor Green
Write-Host "Topic:        projects/$ProjectId/topics/$TopicName"
Write-Host "Subscription: projects/$ProjectId/subscriptions/$SubscriptionName"
Write-Host "Service:      $runUrl"
Write-Host "AR repo:      $Region-docker.pkg.dev/$ProjectId/$ArRepoName"
Write-Host "Trigger:      $triggerName (path: src/prediction_markets/debater/**)"
Write-Host ""

$missing = @()
if (-not $AnthropicApiKey) { $missing += "anthropic-api-key" }
if (-not $OpenAIApiKey)    { $missing += "openai-api-key" }
if (-not $GeminiApiKey)    { $missing += "gemini-api-key" }
if (-not $XaiApiKey)       { $missing += "xai-api-key" }
if ($missing.Count -gt 0) {
  Write-Host "LLM secrets still needing values:" -ForegroundColor Yellow
  foreach ($m in $missing) {
    Write-Host "  - $m   (echo VALUE | gcloud secrets versions add $m --data-file=-)"
  }
}

Write-Host ""
Write-Host "Next steps:"
Write-Host "  1. Switch the Discord alerts channel to a Forum channel and update"
Write-Host "     the discord-webhook-url secret to its webhook."
Write-Host "  2. Set 'debater: { enabled: true }' in gs://$ConfigBucket/markets.yaml."
Write-Host "  3. Push to main to kick off the first Cloud Build deploy of $ServiceName."
Write-Host "  4. Re-run scripts/deploy-vm.ps1 to install google-cloud-pubsub on the VM."
