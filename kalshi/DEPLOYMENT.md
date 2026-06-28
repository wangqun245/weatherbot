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

The production config uses KAUS observations and trades only during Austin
hours 12 through 16. When a managed batch starts, `once` remains alive until
the target fills or its 40-minute order-management window expires.

## Continuous operation

Install `kalshi/kalshi-weather.service.example` as
`/etc/systemd/system/kalshi-weather.service`, review paths and user, then:

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now kalshi-weather
sudo journalctl -u kalshi-weather -f
```

Each individual order targets 10 contracts but is reduced as necessary so its
notional never exceeds $5. Adjacent-YES entries always request the same number
of contracts on both legs. They only cross when both live books have 1:1 depth,
each leg is at or below $0.85, and their combined price is below $0.90. If only
one leg fills, a GTC balance-repair order is posted for the exact share deficit.
All resting orders carry the batch expiration time and are also explicitly
cancelled when the 40-minute window closes or the next hourly model output
replaces the batch.
