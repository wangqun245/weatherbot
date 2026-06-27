"""Compatibility launcher for the Kalshi weather trader.

The Polymarket weather implementation was intentionally removed on the kalshi
branch. Use ``kalshi_weather_trader.py`` or this compatibility entry point.
"""

from kalshi_weather_trader import main


if __name__ == "__main__":
    main()
