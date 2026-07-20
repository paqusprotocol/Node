#!/usr/bin/env bash
set -euo pipefail

purge=0
if [[ "${1:-}" == "--purge" ]]; then
    purge=1
elif [[ -n "${1:-}" ]]; then
    echo "usage: daemon/macos/uninstall.sh [--purge]" >&2
    exit 2
fi
if [[ $(uname -s) != Darwin ]]; then
    echo "this uninstaller must run on macOS" >&2
    exit 1
fi

base_dir="$HOME/Library/Application Support/Paqus"
plist="$HOME/Library/LaunchAgents/io.paqus.paqusd.plist"
launchctl bootout "gui/$(id -u)/io.paqus.paqusd" 2>/dev/null || true
rm -f "$plist" "$base_dir/bin/paqusd"

if (( purge == 1 )); then
    rm -rf "$base_dir" "$HOME/Library/Logs/Paqus"
    echo "paqusd removed with configuration and blockchain data"
else
    echo "paqusd removed; configuration and blockchain data were preserved in $base_dir"
fi
