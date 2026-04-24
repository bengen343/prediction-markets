#!/bin/bash
# VM startup script. Runs on first boot (and on subsequent boots - must be idempotent).
# Only responsible for baseline OS setup. Code deployment is handled by deploy-vm.ps1.

set -euo pipefail

export DEBIAN_FRONTEND=noninteractive

apt-get update
apt-get install -y --no-install-recommends \
  python3 \
  python3-pip \
  python3-venv \
  git \
  ca-certificates

mkdir -p /opt/collector
