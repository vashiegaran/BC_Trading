# PM2 setup — solana-memecoin-bot (Linux)

PM2 manages the Rust release binary as a long-running, auto-restarting process
with timestamped logs.

## 1. One-time install (on the server)

```bash
# Node.js 20+ (Debian/Ubuntu example)
curl -fsSL https://deb.nodesource.com/setup_20.x | sudo -E bash -
sudo apt-get install -y nodejs

# PM2
sudo npm install -g pm2

# Boot autostart — run the sudo command that PM2 prints
pm2 startup
```

### Log rotation (recommended)
```bash
pm2 install pm2-logrotate
pm2 set pm2-logrotate:max_size 50M
pm2 set pm2-logrotate:retain 14
pm2 set pm2-logrotate:compress true
pm2 set pm2-logrotate:rotateInterval '0 0 * * *'   # midnight daily
```

## 2. Build the release binary

```bash
sudo apt install -y protobuf-compiler   # if not already
export PROTOC=$(which protoc)
cargo build --release
```

PM2 runs `target/release/solana-memecoin-bot` from this repo root.

## 3. Start

```bash
mkdir -p logs
pm2 start ecosystem.config.js
pm2 save                  # persist process list across reboots
```

## 4. Daily operations

```bash
pm2 status                            # all apps + cpu/mem
pm2 logs solana-memecoin-bot          # tail combined logs
pm2 logs solana-memecoin-bot --err    # errors only
pm2 logs --lines 200                  # last 200 lines
pm2 monit                             # live dashboard
pm2 describe solana-memecoin-bot      # full process details
```

## 5. Restart after a rebuild

```bash
export PROTOC=$(which protoc)
cargo build --release
pm2 restart solana-memecoin-bot
```

## 6. Stop / remove

```bash
pm2 stop solana-memecoin-bot
pm2 delete solana-memecoin-bot
pm2 save
```

## What the config does

See [ecosystem.config.js](ecosystem.config.js). Highlights:

- `instances: 1`, `exec_mode: fork` — bot is stateful, never cluster.
- `autorestart: true`, `min_uptime: 30s`, `max_restarts: 10` — recovers from
  transient errors, refuses to crash-loop.
- `max_memory_restart: 2G` — recycles the process on memory leak.
- `stop_exit_codes: [0]` — clean exits are not restarted.
- `kill_timeout: 15000` — 15 s for graceful SIGTERM (Supabase flush) before
  SIGKILL.
- Logs: `./logs/bot.out.log`, `./logs/bot.err.log`, timestamped per line.
- `.env` is loaded by the bot itself — not duplicated in PM2 config.

## Verifying autostart

After `pm2 save` and the `pm2 startup` sudo command:

```bash
sudo reboot
# after reboot:
pm2 list            # bot should already be `online`
```

If not, run `pm2 resurrect && pm2 save` and re-check `systemctl status pm2-<user>`.

## Coexistence with the existing systemd setup

This repo also ships [setup_systemd.sh](setup_systemd.sh). Pick **one**
supervisor — running both will fight over the binary. To switch from systemd
to PM2:

```bash
sudo systemctl stop solana-memecoin-bot
sudo systemctl disable solana-memecoin-bot
pm2 start ecosystem.config.js && pm2 save
```
