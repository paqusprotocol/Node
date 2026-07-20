#!/usr/bin/env bash
set -euo pipefail

purge=0
if [[ "${1:-}" == "--purge" ]]; then
    purge=1
elif [[ -n "${1:-}" ]]; then
    echo "usage: sudo daemon/linux/uninstall.sh [--purge]" >&2
    exit 2
fi
if [[ $(id -u) -ne 0 ]]; then
    echo "paqusd system uninstall requires root; run with sudo" >&2
    exit 1
fi

systemctl disable --now paqusd.service 2>/dev/null || true
rm -f /etc/systemd/system/paqusd.service
rm -f /usr/local/bin/paqusd
rm -f /usr/local/libexec/paqusd-stop
systemctl daemon-reload

if (( purge == 1 )); then
    rm -rf /etc/paqus /var/lib/paqus
    userdel paqus 2>/dev/null || true
    groupdel paqus 2>/dev/null || true
    echo "paqusd removed with configuration and blockchain data"
else
    echo "paqusd removed; /etc/paqus and /var/lib/paqus were preserved"
fi
