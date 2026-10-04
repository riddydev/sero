"""Serotonin: a Telegram football statistics bot.

The bot intentionally keeps its public surface small:
  /help
  /team <team name>
  /h2h <team 1> vs <team 2>

Runtime credentials are read from Replit Secrets / environment variables and
are never included in log messages or user-facing errors.

Key design choices:
  - Persistent SQLite cache for team lookups and match lists
  - Per-user rate limiting
  - Single-instance lock to avoid Telegram polling conflicts
  - Batched competition fetches to respect API rate limits
"""
