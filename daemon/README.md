# paqusd installers

Build `paqusd` on the operating system where it will run:

```bash
cargo build --release --locked --bin paqusd
```

Then use the installer for that platform:

```text
linux/install.sh       systemd system service
macos/install.sh       launchd user agent
windows/install.ps1    Windows Scheduled Task
```

Mining is disabled by default. Existing node configuration is preserved during
reinstallation. Running daemons are stopped cleanly and restarted during an
upgrade. Each platform also includes an uninstaller; configuration and
blockchain data are preserved unless the explicit `--purge` or `-Purge` option
is supplied.
