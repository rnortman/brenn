# Brenn fail2ban Configuration

## Setup

1. Copy the filter to fail2ban's filter directory:
   ```
   sudo cp brenn.conf /etc/fail2ban/filter.d/brenn.conf
   ```

2. Copy the jail configuration:
   ```
   sudo cp brenn.local /etc/fail2ban/jail.d/brenn.local
   ```

3. **Edit the jail** — adjust `logpath` to match your deployment's security log location:
   ```ini
   logpath = /var/log/brenn/security.log
   ```
   The default (`./logs/security.log`) matches the dev default from `ObsConfig`.
   Brenn writes to a stable filename (no date suffix), so fail2ban can monitor
   the file directly without globs.

4. Restart fail2ban:
   ```
   sudo systemctl restart fail2ban
   ```

5. Verify the jail is active:
   ```
   sudo fail2ban-client status brenn
   ```

## How It Works

Brenn emits security events to a dedicated JSON log file (`security.log`). Every security event — failed auth, schema violations, unrecognized URLs, malformed messages — includes `"security_event": true` and an `"ip"` field.

The fail2ban filter matches any line containing both fields. After `maxretry` (default: 5) events from the same IP within `findtime` (default: 10 minutes), the IP is banned for `bantime` (default: 1 hour).

## Field Order Dependency

The regex in `brenn.conf` depends on `"security_event"` appearing before `"ip"` in the JSON output. This ordering is determined by the field declaration order in the `tracing::warn!` call in `brenn-lib/src/obs/security.rs`. If that code is refactored, verify the regex still matches.

## Tuning

- `maxretry`: Lower for stricter banning (3 = aggressive, 10 = lenient).
- `findtime`: Window in seconds for counting retries.
- `bantime`: Ban duration in seconds. Use `-1` for permanent bans.
- `action`: Default uses iptables. Adjust for your firewall (nftables, ufw, etc.).
