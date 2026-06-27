# Kalshi weather trader deployment

Copy every path listed in `kalshi/deployment_manifest.txt` while preserving the
directory structure.

## Install

```bash
cd /opt/weatherbot
python3 -m venv .venv
.venv/bin/pip install -r requirements-kalshi.txt
cp kalshi.env.example .env
mkdir -p secrets kalshi/runtime
chmod 700 secrets
chmod 600 .env secrets/kalshi-private-key.pem
```

Edit `.env` with the Kalshi key ID and absolute private-key path. Never commit
the private key.

## Read-only validation

```bash
.venv/bin/python kalshi_weather_trader.py status --config kalshi_weather_config.json
```

`status` authenticates and reads the balance when production trading is
enabled, but never submits an order.

## One real trading cycle

```bash
.venv/bin/python kalshi_weather_trader.py once --config kalshi_weather_config.json
```

The production config trades only during fixed-CST hours 12 through 16. It may
therefore exit without an order outside the window or when price protections
reject the selected plan.

## Continuous operation

Install `kalshi/kalshi-weather.service.example` as
`/etc/systemd/system/kalshi-weather.service`, review paths and user, then:

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now kalshi-weather
sudo journalctl -u kalshi-weather -f
```

Each individual order targets 10 contracts but is reduced as necessary so its
notional never exceeds $5. A two-adjacent-YES plan can submit two separately
capped orders.
