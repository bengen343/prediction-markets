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

# 5. systemd units (always overwrite so changes land)
echo "Installing systemd units..."
cp "${STAGING}/systemd/"*.service /etc/systemd/system/
if ls "${STAGING}/systemd/"*.timer >/dev/null 2>&1; then
  cp "${STAGING}/systemd/"*.timer /etc/systemd/system/
fi
systemctl daemon-reload
for svc in kalshi-collector.service polymarket-collector.service; do
  if [ -f "/etc/systemd/system/${svc}" ]; then
    systemctl enable "${svc}" >/dev/null
  fi
done
# enable --now arms the timer immediately and is idempotent on re-runs.
for timer in notifier.timer kalshi-resolver.timer polymarket-resolver.timer; do
  if [ -f "/etc/systemd/system/${timer}" ]; then
    systemctl enable --now "${timer}" >/dev/null
  fi
done

# 6. Restart units + report status
echo "Restarting units..."
for svc in kalshi-collector.service polymarket-collector.service; do
  if [ -f "/etc/systemd/system/${svc}" ]; then
    systemctl restart "${svc}"
  fi
done
sleep 2
for svc in kalshi-collector.service polymarket-collector.service; do
  if [ -f "/etc/systemd/system/${svc}" ]; then
    systemctl --no-pager status "${svc}" || true
  fi
done
for timer in notifier.timer kalshi-resolver.timer polymarket-resolver.timer; do
  if [ -f "/etc/systemd/system/${timer}" ]; then
    systemctl --no-pager status "${timer}" || true
  fi
done

echo "=== install-on-vm.sh: done ==="
