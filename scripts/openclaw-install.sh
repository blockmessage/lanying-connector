#!/usr/bin/env bash
set -euo pipefail

INSTALLER_URL="${OPENCLAW_INSTALLER_URL:-https://openclaw.ai/install.sh}"
INSTALL_ARGS="${OPENCLAW_INSTALL_ARGS:---no-prompt --no-onboard}"

export OPENCLAW_NO_PROMPT=1
export OPENCLAW_NO_ONBOARD=1

if command -v curl >/dev/null 2>&1; then
  curl -fsSL --proto '=https' --tlsv1.2 "$INSTALLER_URL" | bash -s -- $INSTALL_ARGS
elif command -v wget >/dev/null 2>&1; then
  wget -qO- "$INSTALLER_URL" | bash -s -- $INSTALL_ARGS
else
  echo "错误: 未找到 curl 或 wget，无法下载 OpenClaw 安装脚本。" >&2
  exit 1
fi

openclaw setup

OPENCLAW_TOKEN="$(openssl rand -hex 32)"
mkdir -p "${HOME}/.openclaw"
cat > "${HOME}/.openclaw/config.json" <<EOF
{
  "agents": {
    "defaults": {
      "model": {
        "primary": "openai/gpt-4o-mini"
      },
      "models": {
        "openai/gpt-4o-mini": {}
      },
      "workspace": "/home/openclaw/.openclaw/workspace",
      "compaction": {
        "mode": "safeguard"
      },
      "maxConcurrent": 4,
      "subagents": {
        "maxConcurrent": 8
      }
    }
  },
  "tools": {
    "profile": "messaging"
  },
  "messages": {
    "ackReactionScope": "group-mentions"
  },
  "commands": {
    "native": "auto",
    "nativeSkills": "auto",
    "restart": false,
    "ownerDisplay": "raw"
  },
  "session": {
    "dmScope": "per-channel-peer"
  },
  "gateway": {
    "port": 18789,
    "mode": "local",
    "bind": "loopback",
    "auth": {
      "mode": "token",
      "token": "$OPENCLAW_TOKEN"
    },
    "tailscale": {
      "mode": "off",
      "resetOnExit": false
    },
    "nodes": {
      "denyCommands": [
        "camera.snap",
        "camera.clip",
        "screen.record",
        "contacts.add",
        "calendar.add",
        "reminders.add",
        "sms.send"
      ]
    }
  }
}
EOF
