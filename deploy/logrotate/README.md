# Brenn logrotate Configuration

## Setup

1. Copy the logrotate config:
   ```
   sudo cp deploy/logrotate/brenn /etc/logrotate.d/brenn
   ```

2. Edit paths and user/group if needed. Defaults assume:
   - Logs in `/var/log/brenn/`
   - PID file at `/run/brenn/brenn.pid`
   - `brenn` user/group

3. Configure Brenn to write a PID file by adding to your config TOML:
   ```toml
   [server]
   pid_file = "/run/brenn/brenn.pid"
   ```

4. Ensure the PID directory exists (typically via systemd's `RuntimeDirectory=brenn`).

## How It Works

Brenn writes to stable log filenames (`brenn.log`, `security.log`) — no date suffixes.
logrotate handles rotation via the standard rename + signal pattern:

1. logrotate renames `brenn.log` to `brenn.log.1`
2. logrotate sends `SIGHUP` to Brenn via the PID file
3. Brenn reopens the log at the original path (using the `reopen` crate)
4. No log entries are lost

Old logs are compressed after one rotation cycle (`delaycompress`) and retained for 30 days.

## Verify

```
sudo logrotate --debug /etc/logrotate.d/brenn
```
