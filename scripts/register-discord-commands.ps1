#Requires -Version 5.1
<#
.SYNOPSIS
  One-time registration of the /debate slash command with Discord.

.DESCRIPTION
  Registers the /debate command for the Discord application identified by
  -ApplicationId. Registers as a guild command (instant propagation) when
  -GuildId is supplied; otherwise as a global command (~1 hour propagation).

  The bot token must already be in Secret Manager as discord-bot-token, OR
  passed inline via -BotToken.

.EXAMPLE
  .\register-discord-commands.ps1 -ApplicationId "12345..." -GuildId "67890..."

.EXAMPLE
  .\register-discord-commands.ps1 -ApplicationId "12345..." -BotToken "MTk0..."
#>

param(
  [Parameter(Mandatory = $true)]
  [string]$ApplicationId,

  [string]$GuildId = "",
  [string]$BotToken = ""
)

$ErrorActionPreference = "Stop"

if (-not $BotToken) {
  Write-Host "Fetching bot token from Secret Manager..."
  $BotToken = & gcloud secrets versions access latest --secret discord-bot-token 2>$null
  if (-not $BotToken) {
    throw "discord-bot-token secret is empty. Pass -BotToken inline or populate the secret first."
  }
}

$endpoint = if ($GuildId) {
  "https://discord.com/api/v10/applications/$ApplicationId/guilds/$GuildId/commands"
} else {
  "https://discord.com/api/v10/applications/$ApplicationId/commands"
}

$command = @{
  name        = "debate"
  description = "Run an LLM debate. Defaults to the most recent alert in the thread."
  type        = 1  # CHAT_INPUT
  options     = @(
    @{
      name        = "question"
      description = "Custom question to debate. Omit to use the most recent alert in the thread."
      type        = 3  # STRING
      required    = $false
      max_length  = 500
    }
  )
} | ConvertTo-Json -Depth 6

Write-Host "Registering /debate at $endpoint..."

# PowerShell 5.1 Invoke-RestMethod handles content-type quirks fine.
$headers = @{
  "Authorization" = "Bot $BotToken"
  "Content-Type"  = "application/json; charset=utf-8"
}

# POST with the same name does an upsert in Discord's API — re-runnable.
$response = Invoke-RestMethod -Method Post -Uri $endpoint -Headers $headers -Body ([System.Text.Encoding]::UTF8.GetBytes($command))
Write-Host "Registered:" -ForegroundColor Green
$response | ConvertTo-Json -Depth 6
