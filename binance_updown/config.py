"""Minimal config shim. Only the optional Postgres tick mirror reads this."""
import os
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_NAME = os.getenv("DB_NAME", "updown")
DB_USER = os.getenv("DB_USER", "updown")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")
