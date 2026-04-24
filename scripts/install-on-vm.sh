#!/bin/bash
# Installs / updates the prediction-markets collector on this VM.
# Invoked by scripts/deploy-vm.ps1 via sudo. Idempotent.

set -euo pipefail

STAGING="${1:?staging dir required as first argument}"
INSTALL_DIR="/opt/collector"
VENV_DIR="${INSTALL_DIR}/venv"
SERVICE_USER="collector"
UV_BIN="/usr/local/bin/uv"

echo "=== install-on-vm.sh: staging=${STAGING} install=${INSTALL_DIR} ==="

# 0. Base OS deps not in the startup script
if ! command -v rsync >/dev/null 2>&1; then
  apt-get update -qq
  apt-get install -y -qq rsync
fi

# 1. Service user (no login shell, home dir for any tool caches)
if ! id -u "${SERVICE_USER}" >/dev/null 2>&1; then
  echo "Creating service user ${SERVICE_USER}..."
  useradd -r -m -d /var/lib/collector -s /usr/sbin/nologin "${SERVICE_USER}"
fi

# 2. uv installed system-wide so sudo -u <user> can find it
if ! [ -x "${UV_BIN}" ]; then
  echo "Installing uv..."
  export UV_INSTALL_DIR=/usr/local/bin
  curl -LsSf https://astral.sh/uv/install.sh | sh
fi

# 3. Sync code into install dir
echo "Syncing code..."
mkdir -p "${INSTALL_DIR}"
rsync -a --delete \
  --exclude='venv' \
  --exclude='__pycache__' \
  "${STAGING}/src" \
  "${STAGING}/pyproject.toml" \
  "${INSTALL_DIR}/"
chown -R "${SERVICE_USER}:${SERVICE_USER}" "${INSTALL_DIR}"

# 4. venv + deps (idempotent: uv reuses existing venv)
if [ ! -f "${VENV_DIR}/bin/python" ]; then
  echo "Creating venv..."
  sudo -u "${SERVICE_USER}" "${UV_BIN}" venv "${VENV_DIR}" --python 3.11
fi
echo "Installing deps..."
sudo -u "${SERVICE_USER}" bash -c "cd ${INSTALL_DIR} && VIRTUAL_ENV=${VENV_DIR} ${UV_BIN} pip install -e ."

# 5. systemd unit (always overwrite so unit changes land)
echo "Installing systemd unit..."
cp "${STAGING}/systemd/kalshi-collector.service" /etc/systemd/system/kalshi-collector.service
systemctl daemon-reload
systemctl enable kalshi-collector.service >/dev/null

# 6. Restart service + report status
echo "Restarting service..."
systemctl restart kalshi-collector.service
sleep 2
systemctl --no-pager status kalshi-collector.service || true

echo "=== install-on-vm.sh: done ==="
