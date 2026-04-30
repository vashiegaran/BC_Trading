// PM2 ecosystem config for solana-memecoin-bot.
// Manages the Rust release binary as a long-running process with
// auto-restart, log rotation hooks, and graceful shutdown.
//
// Usage:
//   npm i -g pm2
//   pm2 start ecosystem.config.js
//   pm2 logs solana-memecoin-bot
//   pm2 save                 # persist to disk
//   pm2 startup              # generate OS-level autostart (Linux/macOS)
//   pm2-startup install      # Windows equivalent (npm i -g pm2-windows-startup)
//
// On Windows, ensure PROTOC is available at build time only — runtime does
// not need it. The .env file is loaded by the bot itself (not PM2), so we do
// not duplicate env vars here. Only PM2-specific overrides go in `env`.

const path = require('path');
const isWin = process.platform === 'win32';

module.exports = {
  apps: [
    {
      name: 'solana-memecoin-bot',
      script: isWin
        ? path.join(__dirname, 'target', 'release', 'solana-memecoin-bot.exe')
        : path.join(__dirname, 'target', 'release', 'solana-memecoin-bot'),
      cwd: __dirname,
      interpreter: 'none',          // run binary directly, not via node
      instances: 1,                 // single-instance — bot is stateful
      exec_mode: 'fork',            // not cluster
      autorestart: true,
      watch: false,                 // never restart on file change
      max_memory_restart: '2G',     // restart if RSS exceeds 2 GB
      min_uptime: '30s',            // crash-loop guard
      max_restarts: 10,             // give up after 10 restarts in min_uptime window
      restart_delay: 5000,          // 5s between restarts
      kill_timeout: 15000,          // 15s for graceful SIGTERM before SIGKILL
      stop_exit_codes: [0],         // exit 0 = intentional, do not restart
      time: true,                   // prefix each log line with timestamp
      merge_logs: true,
      out_file: path.join(__dirname, 'logs', 'bot.out.log'),
      error_file: path.join(__dirname, 'logs', 'bot.err.log'),
      log_date_format: 'YYYY-MM-DD HH:mm:ss.SSS',
      env: {
        RUST_LOG: 'info',
        RUST_BACKTRACE: '1',
      },
      env_production: {
        RUST_LOG: 'info',
        RUST_BACKTRACE: '1',
      },
    },
  ],
};
