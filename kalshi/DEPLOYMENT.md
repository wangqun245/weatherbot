# Kalshi weather trader deployment

The production entry point is `kalshi_weather_trader.py run`. It runs all 16
configured stations continuously. `KAUS`, `KLAS`, and `KMIA` are live; the
remaining stations stay in paper mode.

## 1. Copy the application

Copy every path in `kalshi/deployment_manifest.txt` to the server while
preserving its relative path. The examples below use `/opt/weatherbot`.

```bash
sudo mkdir -p /opt/weatherbot
sudo chown -R ubuntu:ubuntu /opt/weatherbot
cd /opt/weatherbot
```

After copying, verify that the model exists:

```bash
test -f kalshi/models/rolling_6y_holdout_lag10_speci_two_asos_16stations_20260628/lightgbm_metar_high_rolling_6y_best.pkl
```

## 2. Create the Python environment

```bash
cd /opt/weatherbot
python3 -m venv .venv
.venv/bin/python -m pip install --upgrade pip
.venv/bin/pip install -r requirements-kalshi.txt
mkdir -p secrets kalshi/runtime
chmod 700 secrets
```

## 3. Configure credentials

```bash
cp kalshi.env.example .env
nano .env
```

Set the Kalshi API key ID and absolute private-key path. Telegram values are
optional. Place the PEM key at the configured location, then protect both
files:

```bash
chmod 600 .env secrets/kalshi-private-key.pem
```

Never commit or copy the private key into the repository.

## 4. Validate before starting the service

Run these commands as the same Linux user that will own the service:

```bash
cd /opt/weatherbot
.venv/bin/python -m py_compile kalshi_weather_trader.py kalshi_execution.py
.venv/bin/python kalshi_weather_trader.py status --config kalshi_weather_config.json
```

`status` validates production credentials, reads the account balance, and
checks all configured Kalshi series. It does not submit orders.

For a foreground continuous test:

```bash
set -a
source .env
set +a
.venv/bin/python kalshi_weather_trader.py run --config kalshi_weather_config.json
```

Stop the foreground test with `Ctrl+C`.

## 5. Install the systemd service

The supplied unit assumes:

- Linux user: `ubuntu`
- project directory: `/opt/weatherbot`
- environment file: `/opt/weatherbot/.env`

If the server uses another user or directory, edit the unit first.

```bash
sudo cp kalshi/kalshi-weather.service.example /etc/systemd/system/kalshi-weather.service
sudo systemctl daemon-reload
sudo systemctl enable kalshi-weather.service
sudo systemctl start kalshi-weather.service
```

Confirm that it remains running:

```bash
sudo systemctl status kalshi-weather.service --no-pager -l
sudo journalctl -u kalshi-weather.service -n 200 --no-pager
```

Follow logs continuously:

```bash
sudo journalctl -u kalshi-weather.service -f
tail -F /opt/weatherbot/kalshi/runtime/kalshi_weather_trader.log
```

The service uses `Restart=always`, so it restarts after an unexpected exit and
starts automatically after a server reboot.

## 6. Updating the deployed code

After copying updated files:

```bash
cd /opt/weatherbot
.venv/bin/pip install -r requirements-kalshi.txt
sudo systemctl restart kalshi-weather.service
sudo systemctl status kalshi-weather.service --no-pager -l
sudo journalctl -u kalshi-weather.service -n 100 --no-pager
```

If the unit itself changes:

```bash
sudo cp kalshi/kalshi-weather.service.example /etc/systemd/system/kalshi-weather.service
sudo systemctl daemon-reload
sudo systemctl restart kalshi-weather.service
```

## 7. Operational commands

```bash
# Stop
sudo systemctl stop kalshi-weather.service

# Start
sudo systemctl start kalshi-weather.service

# Restart
sudo systemctl restart kalshi-weather.service

# Disable automatic startup
sudo systemctl disable --now kalshi-weather.service

# Show failures from the current boot
sudo journalctl -u kalshi-weather.service -b -p warning
```

Runtime state and audit files are stored under `kalshi/runtime`. Preserve this
directory during deployment so completed hourly windows and managed orders are
not forgotten during a restart.

## Production limits

- Live stations: `KAUS`, `KLAS`, `KMIA`
- Other 13 stations: paper trade
- Default target: 10 contracts
- Maximum cost budget: $10 per leg
- Maximum price per contract: $0.85
- Adjacent YES total-price threshold: below $0.90
- Adjacent YES orders always use equal contract counts
- Order-management window: 40 minutes
