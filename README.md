# prediction-markets

Personal data-collection + alerting pipeline for prediction-market trades.
Streams Kalshi and Polymarket trades into BigQuery, runs a scheduled
anomaly query, and pings Discord on unusual activity. Runs on a single
GCE `e2-micro` (Always Free tier).

## Useful commands

### Connect to the VM

```sh
gcloud compute ssh collector-vm --zone us-west1-a --project XXX
```

### Monitor the collectors (live trade ingestion)

```sh
sudo journalctl -u kalshi-collector -f
sudo journalctl -u polymarket-collector -f
sudo journalctl -u notifier -f
```

### Run a resolver ad-hoc with human-readable output

JSON_LOGS=0 swaps structlog's JSON renderer for the dev console renderer;
`python -u` keeps stdout unbuffered so output streams instead of waiting
for the pagination loop to finish.

```sh
sudo -u collector env GCE_METADATA_MTLS_MODE=none JSON_LOGS=0 \
  /opt/collector/venv/bin/python -u -m prediction_markets.kalshi.resolver

sudo -u collector env GCE_METADATA_MTLS_MODE=none JSON_LOGS=0 \
  /opt/collector/venv/bin/python -u -m prediction_markets.polymarket.resolver
```

Cap the work for a smoke test. Different knobs by source: Polymarket is a flat
list of markets, so `MARKETS_LIMIT` caps total markets; Kalshi is hierarchical
(category → series → markets), so `SERIES_LIMIT` caps the number of series
resolved (each series has 1+ markets).

```sh
sudo -u collector env GCE_METADATA_MTLS_MODE=none JSON_LOGS=0 SERIES_LIMIT=3 \
  /opt/collector/venv/bin/python -u -m prediction_markets.kalshi.resolver

sudo -u collector env GCE_METADATA_MTLS_MODE=none JSON_LOGS=0 MARKETS_LIMIT=10 \
  /opt/collector/venv/bin/python -u -m prediction_markets.polymarket.resolver
```

### Trigger a resolver via systemd (writes to BQ + GCS for real)

```sh
sudo systemctl start kalshi-resolver.service
sudo systemctl start polymarket-resolver.service
```

### Inspect Polymarket connection health (without restarting)

```sh
# All 32 shards subscribed at least once in the last hour?
sudo journalctl -u polymarket-collector --since "1 hour ago" -o cat \
  | grep '"event": "polymarket.subscribed"' \
  | grep -oE '"idx": [0-9]+' | sort -u | wc -l

# Any flapping?
sudo journalctl -u polymarket-collector --since "1 hour ago" -o cat \
  | grep -E '"event": "polymarket.(connection_dropped|connection_reconnect)"'
```

### BigQuery sanity checks

```sh
# Recent trade volume per source
bq query --project_id=prediction-markets-51920 --use_legacy_sql=false \
  "SELECT source, COUNT(*) AS trades, MIN(ts) AS first, MAX(ts) AS latest \
   FROM prediction_markets.trades \
   WHERE DATE(ts) = CURRENT_DATE() GROUP BY source"

# Latest alerts
bq query --project_id=prediction-markets-51920 --use_legacy_sql=false \
  "SELECT detected_at, source, market_id, title, side, size, price, reason \
   FROM prediction_markets.alerts \
   ORDER BY detected_at DESC LIMIT 20"
```

### Inspect / edit the human config

```sh
gcloud storage cat gs://prediction-markets-51920-config/markets.yaml
gcloud storage cp markets.yaml gs://prediction-markets-51920-config/markets.yaml
```

### Deploy code changes from the workstation

```powershell
.\scripts\deploy-vm.ps1
```

## Architecture

- **Compute:** one GCE `e2-micro` VM `collector-vm` in `us-west1`. All
  services run under systemd as the `collector` user.
- **Collectors (long-running):** `kalshi-collector` (one WebSocket),
  `polymarket-collector` (32 sharded WebSockets - the Polymarket WS
  server has an undocumented subscribe-payload size cap). Both write
  trades into the shared `trades` BQ table via a batching `BqWriter`.
- **Resolvers (daily):** `kalshi-resolver.timer` at 01:00 America/Denver,
  `polymarket-resolver.timer` at 03:00. Discover open markets in tracked
  Kalshi categories / Polymarket tags and write the subscription list to
  GCS, which the collectors read.
- **Anomaly detection:** scheduled query `detect_anomalies` in BQ
  Console, every 15 min. Inserts flagged trades into the `alerts` table.
- **Notifier (every 5 min):** `notifier.timer` polls `alerts` for
  unsent rows and posts them to Discord.

## Data flow

```
Kalshi WS  ─┐                         ┌── detect_anomalies (BQ scheduled)
            ├─> trades ───────────────┤
Polymarket  ┘                         └─> alerts ─> notifier ─> Discord
```

## Repository layout

```
scripts/                      provisioning + deploy (PowerShell + bash)
sql/                          BQ DDL files (one per table)
systemd/                      *.service and *.timer units
config/markets.example.yaml   human-edited intent (categories, tag_slugs)
src/prediction_markets/
  shared/                     BqWriter, ConfigWatcher, secrets, log, subscriptions helper
  kalshi/                     auth, REST, WS client, resolver
  polymarket/                 gamma client, WS client, resolver
  notifier/                   Discord poster
```

## First-time setup

```powershell
.\scripts\provision-gcp.ps1 -BillingAccountId "0X0X0X-0X0X0X-0X0X0X" `
  -KalshiPrivateKeyFile "$HOME\secrets\kalshi_private.txt" `
  -KalshiApiKeyId "..." `
  -DiscordWebhookUrl "https://discord.com/api/webhooks/..."
.\scripts\deploy-vm.ps1
```

The `detect_anomalies` scheduled query is installed manually in the BQ
Console (the `bq mk --transfer_config` flow choked on PowerShell quoting).
