module.exports = {
  apps: [{
    name: 'ethbot-live',
    script: 'eth_bot.py',
    args: '--live --yes',
    interpreter: './venv/bin/python',
    cwd: '/root/ethbot',
    instances: 1,
    autorestart: true,
    max_memory_restart: '1G',
    env: {
      PYTHONUNBUFFERED: '1'
    }
  }]
};
