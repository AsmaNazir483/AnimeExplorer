"""SQLite storage for AniSort Lab.

Everything the app shows comes from this file (data/anime.db), so a failed
scrape can never take the rest of the project down with it.
"""
import json
import os
import sqlite3
import threading

DB_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "anime.db")
JSON_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "anime.json")

# Columns shown in the UI, in order. anime_id stays hidden but is the unique key.
COLUMNS = [
    "title", "type", "episodes", "episode_length", "rating", "rank",
    "popularity", "users", "year", "status", "age_rating",
]
EXTRA_COLUMNS = ["anime_id", "source_url", "scraped_at"]
ALL_COLUMNS = ["anime_id"] + COLUMNS + ["source_url", "scraped_at"]

# Column kinds, used later by sorting, searching and filters.
COLUMN_KINDS = {
    "title": "text", "type": "category", "episodes": "int", "episode_length": "int",
    "rating": "float", "rank": "int", "popularity": "int", "users": "int",
    "year": "int", "status": "category", "age_rating": "category",
}

SCHEMA = """
CREATE TABLE IF NOT EXISTS anime (
    anime_id       INTEGER PRIMARY KEY,
    title          TEXT,
    type           TEXT,
    episodes       INTEGER,
    episode_length INTEGER,
    rating         REAL,
    rank           INTEGER,
    popularity     INTEGER,
    users          INTEGER,
    year           INTEGER,
    status         TEXT,
    age_rating     TEXT,
    source_url     TEXT,
    scraped_at     TEXT
);
CREATE TABLE IF NOT EXISTS state (
    key   TEXT PRIMARY KEY,
    value TEXT
);
"""


class Database:
    def __init__(self, path=DB_PATH):
        self.path = path
        os.makedirs(os.path.dirname(path), exist_ok=True)
        self._lock = threading.Lock()
        # The scraper runs in a worker thread, so allow cross-thread use
        # and protect every call with a lock.
        self._conn = sqlite3.connect(path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        with self._lock:
            self._conn.executescript(SCHEMA)
            self._conn.commit()

    # ---- basic info -------------------------------------------------
    def count(self):
        with self._lock:
            return self._conn.execute("SELECT COUNT(*) FROM anime").fetchone()[0]

    def close(self):
        with self._lock:
            self._conn.close()

    # ---- scrape progress (so Resume continues from the right page) ---
    def get_saved_page(self, key="last_page"):
        with self._lock:
            row = self._conn.execute(
                "SELECT value FROM state WHERE key = ?", (key,)).fetchone()
        return int(row["value"]) if row else 0

    def set_saved_page(self, page, key="last_page"):
        with self._lock:
            self._conn.execute(
                "INSERT OR REPLACE INTO state(key, value) VALUES (?, ?)",
                (key, str(page)))
            self._conn.commit()

    def reset_progress(self, key="last_page"):
        with self._lock:
            self._conn.execute("DELETE FROM state WHERE key = ?", (key,))
            self._conn.commit()

    # ---- saving ------------------------------------------------------
    def save_page(self, page, records, state_key="last_page"):
        """Insert one page of records and remember the page number.

        INSERT OR IGNORE on anime_id means a record that already exists is
        skipped, so duplicates can never appear. Everything for one page is
        committed together: a crash loses at most that page.
        Returns how many NEW rows were added.
        """
        cols = ["anime_id"] + COLUMNS + ["source_url", "scraped_at"]
        sql = "INSERT OR IGNORE INTO anime ({}) VALUES ({})".format(
            ", ".join(cols), ", ".join("?" for _ in cols))
        with self._lock:
            before = self._conn.execute("SELECT COUNT(*) FROM anime").fetchone()[0]
            self._conn.executemany(
                sql, [tuple(r.get(c) for c in cols) for r in records])
            after = self._conn.execute("SELECT COUNT(*) FROM anime").fetchone()[0]
            if page is not None:
                self._conn.execute(
                    "INSERT OR REPLACE INTO state(key, value) VALUES (?, ?)",
                    (state_key, str(page)))
            self._conn.commit()
        return after - before

    # ---- loading -----------------------------------------------------
    def load_all(self):
        """Return every record as a list of plain dicts (None = empty)."""
        with self._lock:
            rows = self._conn.execute(
                "SELECT {} FROM anime ORDER BY anime_id".format(
                    ", ".join(ALL_COLUMNS))).fetchall()
        return [dict(r) for r in rows]

    # ---- JSON backup / fallback ------------------------------------
    def export_json(self, path=JSON_PATH):
        records = self.load_all()
        tmp = path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(records, f, ensure_ascii=False)
        os.replace(tmp, path)          # never leaves a half-written file
        return len(records)

    def import_json(self, path=JSON_PATH):
        """Load records from a JSON backup into the database."""
        with open(path, "r", encoding="utf-8") as f:
            records = json.load(f)
        added = self.save_page(None, records)
        return added


def load_records_safely(db=None, json_path=JSON_PATH):
    """What the app calls at start-up. It never raises.

    1. Use the database if it has data.
    2. Otherwise try the JSON backup.
    3. Otherwise return an empty list (the UI then says: scrape or load a file).
    """
    try:
        db = db or Database()
        rows = db.load_all()
        if rows:
            return rows
    except Exception:
        pass
    try:
        with open(json_path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return []
