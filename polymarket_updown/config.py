"""Minimal config shim. Set DB_URL to a SQLAlchemy URL (Postgres recommended)."""
import os
DB_URL = os.getenv("DB_URL", "sqlite:///polymarket_updown.sqlite3")
