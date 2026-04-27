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
  type        = 1
  options     = @(
    @{
      name        = "question"
      description = "Custom question to debate. Omit to use the most recent alert in the thread."
      type        = 3
      required    = $false
    }
  )
} | ConvertTo-Json -Depth 6 -Compress

Write-Host "Registering /debate at $endpoint..."
Write-Host "Payload: $command"

# Trim whitespace defensively — secrets pulled via PowerShell pipes sometimes
# carry a trailing newline that the Authorization header rejects silently.
$BotToken = $BotToken.Trim()

# PowerShell 5.1: -ContentType + string body is the most reliable form.
# Bytes-via-Body or Content-Type-in-Headers both have edge-case bugs in 5.1.
try {
  $response = Invoke-RestMethod `
    -Method Post `
    -Uri $endpoint `
    -Headers @{ "Authorization" = "Bot $BotToken" } `
    -ContentType "application/json" `
    -Body $command
  Write-Host "Registered:" -ForegroundColor Green
  $response | ConvertTo-Json -Depth 6
} catch {
  Write-Host "Registration failed." -ForegroundColor Red
  if ($_.Exception.Response) {
    $reader = New-Object System.IO.StreamReader($_.Exception.Response.GetResponseStream())
    $body = $reader.ReadToEnd()
    Write-Host "HTTP $([int]$_.Exception.Response.StatusCode) $($_.Exception.Response.StatusCode):"
    Write-Host $body
  } else {
    Write-Host $_.Exception.Message
  }
  throw
}
