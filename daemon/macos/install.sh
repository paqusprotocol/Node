#!/usr/bin/env bash
set -euo pipefail

repo_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
daemon_dir="$repo_dir/daemon/macos"
app_dir="$HOME/Library/Application Support/Paqus"
bin_dir="$HOME/Library/Application Support/Paqus/bin"
data_dir="$HOME/Library/Application Support/Paqus/data"
config_dir="$HOME/Library/Application Support/Paqus/config"
log_dir="$HOME/Library/Logs/Paqus"
agent_dir="$HOME/Library/LaunchAgents"
binary="$bin_dir/paqusd"
config="$config_dir/node.json"
plist="$agent_dir/io.paqus.paqusd.plist"
start=${1:-}
was_loaded=0

if [[ "$start" != "" && "$start" != "--start" ]]; then
    echo "usage: daemon/macos/install.sh [--start]" >&2
    exit 2
fi
if [[ $(uname -s) != Darwin ]]; then
    echo "this installer must run on macOS" >&2
    exit 1
fi
if [[ ! -x "$repo_dir/target/release/paqusd" ]]; then
    echo "build paqusd first: cargo build --release --locked -p full-node --bin paqusd" >&2
    exit 1
fi

if launchctl print "gui/$(id -u)/io.paqus.paqusd" >/dev/null 2>&1; then
    was_loaded=1
    launchctl bootout "gui/$(id -u)/io.paqus.paqusd"
fi

mkdir -p "$bin_dir" "$data_dir" "$config_dir" "$log_dir" "$agent_dir"
install -m 0755 "$repo_dir/target/release/paqusd" "$binary"

if [[ ! -e "$config" ]]; then
    sed "s#/var/lib/paqus#$data_dir#g" "$daemon_dir/node.json" >"$config"
    chmod 0600 "$config"
else
    echo "preserving existing $config"
fi

sed \
    -e "s#__BINARY__#$binary#g" \
    -e "s#__DATA__#$data_dir#g" \
    -e "s#__CONFIG__#$config#g" \
    -e "s#__LOG__#$log_dir/paqusd.log#g" \
    "$daemon_dir/io.paqus.paqusd.plist" >"$plist"
chmod 0644 "$plist"

if [[ "$start" == "--start" || "$was_loaded" == 1 ]]; then
    launchctl bootstrap "gui/$(id -u)" "$plist"
fi

echo "paqusd installed: $binary"
echo "config: $config"
echo "data: $data_dir"
if [[ "$start" != "--start" && "$was_loaded" == 0 ]]; then
    echo "start with: launchctl bootstrap gui/$(id -u) '$plist'"
fi
