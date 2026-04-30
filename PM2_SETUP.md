# PM2 setup — solana-memecoin-bot

PM2 manages the Rust release binary as a long-running, auto-restarting process
with timestamped logs.

## One-time install

### Windows
```powershell
# Install Node.js 20+ first (https://nodejs.org), then:
npm install -g pm2
npm install -g pm2-windows-startup
pm2-startup install
# (You may need a fresh PowerShell window after install.)
```

### Linux / macOS
```bash
# Install Node.js 20+ first, then:
npm install -g pm2
pm2 startup            # follow the printed sudo command
```

### Optional: log rotation (recommended on both OSes)
```bash
pm2 install pm2-logrotate
pm2 set pm2-logrotate:max_size 50M
pm2 set pm2-logrotate:retain 14
pm2 set pm2-logrotate:compress true
pm2 set pm2-logrotate:rotateInterval '0 0 * * *'   # midnight daily
```

## Build the release binary

```powershell
$env:PROTOC = "C:\Users\vmanogar\protoc\bin\protoc.exe"
cargo build --release
```

The PM2 config points at `target/release/solana-memecoin-bot(.exe)`.

## Start

```powershell
mkdir logs -ErrorAction SilentlyContinue
pm2 start ecosystem.config.js
pm2 save                  # persist process list across reboots
```

## Daily operations

```powershell
pm2 status                          # all apps + cpu/mem
pm2 logs solana-memecoin-bot        # tail combined logs
pm2 logs solana-memecoin-bot --err  # tail errors only
pm2 logs --lines 200                # last 200 lines
pm2 monit                           # live dashboard (cpu, mem, log stream)
pm2 describe solana-memecoin-bot    # full process details
```

## Restart / reload after a rebuild

```powershell
$env:PROTOC = "C:\Users\vmanogar\protoc\bin\protoc.exe"
cargo build --release
pm2 restart solana-memecoin-bot
```

## Stop / remove

```powershell
pm2 stop solana-memecoin-bot
pm2 delete solana-memecoin-bot
pm2 save
```

## What the config does

See `ecosystem.config.js` — highlights:

- `instances: 1`, `exec_mode: fork` — bot is stateful, never cluster.
- `autorestart: true`, `min_uptime: 30s`, `max_restarts: 10` — guards against
  crash-loops while still recovering from transient errors.
- `max_memory_restart: 2G` — recycles the process if it leaks past 2 GB.
- `stop_exit_codes: [0]` — a clean exit (e.g. shutdown signal handled by the
  bot) is treated as intentional and PM2 will NOT restart.
- `kill_timeout: 15000` — allows up to 15 s for the bot to flush in-flight
  Supabase writes before PM2 force-kills.
- `time: true` + `log_date_format` — every log line is prefixed with a
  millisecond timestamp.
- Logs are written to `./logs/bot.out.log` and `./logs/bot.err.log`.

## Verifying autostart

After `pm2 save` + `pm2-startup install` (Windows) or `pm2 startup` (Linux):

1. Reboot the machine.
2. Run `pm2 list` — the bot should already be `online`.
3. If not, re-run `pm2 resurrect` and then `pm2 save` again.
