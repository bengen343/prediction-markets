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
  [string]$BotServiceAccountName = "discord-bot-sa",
  [string]$CollectorServiceAccountName = "collector-sa",
  [string]$ArRepoName = "prediction-markets-debater",
  [string]$ServiceName = "debate-worker",
  [string]$BotServiceName = "discord-bot",
  [string]$TopicName = "debate-requests",
  [string]$SubscriptionName = "debate-worker",
  [string]$BranchPattern = "^main$",

  [string]$AnthropicApiKey = "",
  [string]$OpenAIApiKey = "",
  [string]$GeminiApiKey = "",
  [string]$XaiApiKey = "",
  [string]$DiscordBotPublicKey = "",
  [string]$DiscordBotToken = ""
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
$BotServiceAccountEmail = "$BotServiceAccountName@$ProjectId.iam.gserviceaccount.com"
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

# Discord bot service account also publishes to this topic when /debate fires.
Write-Host "Granting pubsub.publisher to $BotServiceAccountName on $TopicName..."
# Created below in section 6b; the binding is idempotent — safe to add even if
# the SA was just created in this run.

# --- 6. LLM secrets ---

Write-Host "--- LLM API key secrets ---" -ForegroundColor Yellow
Ensure-Secret -Name "anthropic-api-key"      -ValueInline $AnthropicApiKey
Ensure-Secret -Name "openai-api-key"         -ValueInline $OpenAIApiKey
Ensure-Secret -Name "gemini-api-key"         -ValueInline $GeminiApiKey
Ensure-Secret -Name "xai-api-key"            -ValueInline $XaiApiKey
Ensure-Secret -Name "discord-bot-public-key" -ValueInline $DiscordBotPublicKey
Ensure-Secret -Name "discord-bot-token"      -ValueInline $DiscordBotToken

# --- 6b. Discord bot service account ---
#
# Separate identity from debater-sa. Holds: pubsub publisher (publish to
# debate-requests), BQ read (look up alerts by thread_id), Secret Manager
# accessor (read public key + bot token for slash-command registration).

Write-Host "--- Discord bot service account ---" -ForegroundColor Yellow
if (-not (Test-GcloudResource @("iam", "service-accounts", "describe", $BotServiceAccountEmail))) {
  Invoke-Checked {
    gcloud iam service-accounts create $BotServiceAccountName `
      --display-name "Prediction Markets Discord Bot" --quiet
  } "create discord bot service account"
} else {
  Write-Host "Bot SA already exists, skipping create."
}

$botRoles = @(
  "roles/bigquery.dataViewer",
  "roles/bigquery.jobUser",
  "roles/secretmanager.secretAccessor"
)
foreach ($role in $botRoles) {
  Write-Host "  Binding $role to $BotServiceAccountName..."
  Invoke-Checked {
    gcloud projects add-iam-policy-binding $ProjectId `
      --member "serviceAccount:$BotServiceAccountEmail" `
      --role $role --condition=None --quiet | Out-Null
  } "bind $role to bot SA"
}

# Topic-scoped publisher (deferred from section 5 above; SA didn't exist yet).
Invoke-Checked {
  gcloud pubsub topics add-iam-policy-binding $TopicName `
    --member "serviceAccount:$BotServiceAccountEmail" `
    --role roles/pubsub.publisher --quiet | Out-Null
} "grant publisher to bot SA"

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

# --- 7b. Discord bot Cloud Run service (public; signature verification at app layer) ---

Write-Host "--- Discord bot Cloud Run service ---" -ForegroundColor Yellow
if (-not (Test-GcloudResource @("run", "services", "describe", $BotServiceName, "--region", $Region))) {
  Write-Host "Creating $BotServiceName with placeholder image..."
  Invoke-Checked {
    gcloud run deploy $BotServiceName `
      --image $placeholderImage `
      --region $Region `
      --service-account $BotServiceAccountEmail `
      --allow-unauthenticated `
      --max-instances 5 `
      --concurrency 20 `
      --timeout 30 `
      --cpu 1 `
      --memory 256Mi `
      --quiet
  } "deploy discord-bot (initial)"
} else {
  Write-Host "Discord bot service already exists, leaving image untouched (Cloud Build manages it)."
}

$botRunUrl = & gcloud run services describe $BotServiceName --region $Region --format "value(status.url)" 2>$null
if (-not $botRunUrl) { throw "Failed to resolve Cloud Run service URL for $BotServiceName." }
Write-Host "Bot URL: $botRunUrl"
Write-Host "Interaction endpoint to register with Discord: $botRunUrl/interaction"

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

# Delete and recreate so the included_files + substitutions stay in sync
# across re-runs (gcloud doesn't expose `update github` for first-gen triggers).
$triggerExists = Test-NativeSuccess { gcloud builds triggers describe $triggerName }
if ($triggerExists) {
  Write-Host "Replacing existing trigger $triggerName so config stays in sync..."
  Invoke-Checked {
    gcloud builds triggers delete $triggerName --quiet | Out-Null
  } "delete existing Cloud Build trigger"
}

Write-Host "Creating trigger $triggerName..."
$included = "src/prediction_markets/debater/**,src/prediction_markets/discord_bot/**,cloudbuild.yaml,pyproject.toml"
& gcloud builds triggers create github `
  --name $triggerName `
  --repo-owner $GitHubOwner `
  --repo-name $GitHubRepo `
  --branch-pattern $BranchPattern `
  --build-config cloudbuild.yaml `
  --included-files $included `
  --substitutions "_REGION=$Region,_AR_REPO=$ArRepoName,_DEBATER_SVC=$ServiceName,_BOT_SVC=$BotServiceName" `
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

# --- Summary ---

Write-Host ""
Write-Host "=== Done ===" -ForegroundColor Green
Write-Host "Topic:           projects/$ProjectId/topics/$TopicName"
Write-Host "Subscription:    projects/$ProjectId/subscriptions/$SubscriptionName"
Write-Host "Debater service: $runUrl"
Write-Host "Bot service:     $botRunUrl"
Write-Host "Bot interaction: $botRunUrl/interaction"
Write-Host "AR repo:         $Region-docker.pkg.dev/$ProjectId/$ArRepoName"
Write-Host "Trigger:         $triggerName (paths: debater/**, discord_bot/**)"
Write-Host ""

$missing = @()
if (-not $AnthropicApiKey)     { $missing += "anthropic-api-key" }
if (-not $OpenAIApiKey)        { $missing += "openai-api-key" }
if (-not $GeminiApiKey)        { $missing += "gemini-api-key" }
if (-not $XaiApiKey)           { $missing += "xai-api-key" }
if (-not $DiscordBotPublicKey) { $missing += "discord-bot-public-key" }
if (-not $DiscordBotToken)     { $missing += "discord-bot-token" }
if ($missing.Count -gt 0) {
  Write-Host "Secrets still needing values:" -ForegroundColor Yellow
  foreach ($m in $missing) {
    Write-Host "  - $m   (echo VALUE | gcloud secrets versions add $m --data-file=-)"
  }
}

Write-Host ""
Write-Host "Next steps:"
Write-Host "  1. Switch the Discord alerts channel to a Forum channel and update"
Write-Host "     the discord-webhook-url secret to its webhook (already done if"
Write-Host "     debater is already running)."
Write-Host "  2. Push to main to kick off the first Cloud Build deploy of both"
Write-Host "     services. Cloud Build will replace the placeholder images."
Write-Host "  3. Apply the alerts schema change for discord_thread_id:"
Write-Host "     bq query --use_legacy_sql=false ""ALTER TABLE prediction_markets.alerts ADD COLUMN discord_thread_id STRING"""
Write-Host "  4. Create a Discord application at https://discord.com/developers/applications,"
Write-Host "     copy its public key into discord-bot-public-key, copy the bot token"
Write-Host "     into discord-bot-token, and set the application's Interactions URL to:"
Write-Host "     $botRunUrl/interaction"
Write-Host "  5. Register the slash command:"
Write-Host "     .\scripts\register-discord-commands.ps1 -ApplicationId <APP_ID> -GuildId <GUILD_ID>"
Write-Host "  6. To use the bot exclusively (no auto-debate), set in markets.yaml:"
Write-Host "     debater:"
Write-Host "       enabled: true"
Write-Host "       auto_publish: false"
Write-Host "  7. Re-run scripts/deploy-vm.ps1 to push the notifier change that persists"
Write-Host "     discord_thread_id back to alerts (required for /debate without args)."
