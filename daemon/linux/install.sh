#!/usr/bin/env bash
set -euo pipefail

repo_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
daemon_dir="$repo_dir/daemon/linux"
enable_service=0
was_active=0

if [[ "${1:-}" == "--enable" ]]; then
    enable_service=1
elif [[ -n "${1:-}" ]]; then
    echo "usage: sudo daemon/linux/install.sh [--enable]" >&2
    exit 2
fi

if [[ $(id -u) -ne 0 ]]; then
    echo "paqusd system installation requires root; run with sudo" >&2
    exit 1
fi

if ! command -v systemctl >/dev/null 2>&1; then
    echo "systemd is required for the paqusd service installation" >&2
    exit 1
fi

if systemctl is-active --quiet paqusd.service 2>/dev/null; then
    was_active=1
    systemctl stop paqusd.service
fi

cd "$repo_dir"
if [[ ! -x target/release/paqusd ]]; then
    echo "target/release/paqusd is missing; build it before installation:" >&2
    echo "  cargo build --release --locked -p full-node --bin paqusd" >&2
    exit 1
fi

if ! getent group paqus >/dev/null; then
    groupadd --system paqus
fi
if ! id paqus >/dev/null 2>&1; then
    useradd --system --gid paqus --home-dir /var/lib/paqus --shell /usr/sbin/nologin paqus
fi

install -d -m 0755 -o root -g root /etc/paqus
install -d -m 0750 -o paqus -g paqus /var/lib/paqus
install -d -m 0755 -o root -g root /usr/local/libexec
install -m 0755 target/release/paqusd /usr/local/bin/paqusd
install -m 0755 "$daemon_dir/paqusd-stop" /usr/local/libexec/paqusd-stop
install -m 0644 "$daemon_dir/paqusd.service" /etc/systemd/system/paqusd.service

if [[ ! -e /etc/paqus/node.json ]]; then
    install -m 0640 -o root -g paqus "$daemon_dir/node.json" /etc/paqus/node.json
else
    echo "preserving existing /etc/paqus/node.json"
fi

systemctl daemon-reload
if (( enable_service == 1 )); then
    systemctl enable --now paqusd.service
elif (( was_active == 1 )); then
    systemctl start paqusd.service
fi

echo "paqusd installed: /usr/local/bin/paqusd"
echo "config: /etc/paqus/node.json"
echo "data: /var/lib/paqus"
if (( enable_service == 0 && was_active == 0 )); then
    echo "enable with: sudo systemctl enable --now paqusd"
fi
