"""Kitsu scraper for AniSort Lab.

Kitsu (https://kitsu.io) is a public anime website with a free API. It returns
at most 20 anime per request and uses an "offset" to move through the list.

This file has NO PyQt code. The UI only starts it, pauses it, resumes it and
stops it, and listens to its callbacks. Nothing in here raises an exception
into the UI: every problem becomes a message in the log and an on_error call,
and the app keeps using the data already saved in the database.
"""
import os
import re
import threading
from datetime import datetime
from urllib.parse import urlparse, parse_qsl, urlencode, urlunparse

import requests

API_BASE = "https://kitsu.io/api/edge"
ANIME_LIST_URL = API_BASE + "/anime"
PAGE_LIMIT = 20                     # Kitsu's maximum per request (25 gives HTTP 400)
REQUEST_DELAY = 0.6                 # seconds between requests, to be polite
RETRY_WAITS = (2, 4, 8, 16, 32)     # seconds to wait before each retry
HEADERS = {"Accept": "application/vnd.api+json",
           "User-Agent": "AniSortLab-student-project"}
LOG_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "scrape_log.txt")

STATUS_IDLE = "idle"
STATUS_RUNNING = "running"
STATUS_PAUSED = "paused"
STATUS_STOPPED = "stopped"
STATUS_FINISHED = "finished"
STATUS_FAILED = "failed"

NO_RETRY_CODES = (400, 404)         # asking again cannot help


def now_text():
    return datetime.now().isoformat(timespec="seconds")


def _to_int(v):
    try:
        return int(v) if v is not None else None
    except (TypeError, ValueError):
        return None


def _to_float(v):
    try:
        return float(v) if v is not None else None
    except (TypeError, ValueError):
        return None


def _year(start_date):
    """'1998-04-03' -> 1998. Anything else -> None."""
    if isinstance(start_date, str) and len(start_date) >= 4:
        return _to_int(start_date[:4])
    return None


def parse_anime(item, source_url):
    """Turn one Kitsu anime object into one row. Missing fields become None."""
    if not isinstance(item, dict):
        return None
    anime_id = _to_int(item.get("id"))
    attrs = item.get("attributes")
    if anime_id is None or not isinstance(attrs, dict):
        return None
    return {
        "anime_id": anime_id,
        "title": attrs.get("canonicalTitle"),
        "type": attrs.get("subtype"),
        "episodes": _to_int(attrs.get("episodeCount")),
        "episode_length": _to_int(attrs.get("episodeLength")),
        "rating": _to_float(attrs.get("averageRating")),      # Kitsu sends "82.27" as text
        "rank": _to_int(attrs.get("ratingRank")),
        "popularity": _to_int(attrs.get("popularityRank")),
        "users": _to_int(attrs.get("userCount")),
        "year": _year(attrs.get("startDate")),
        "status": attrs.get("status"),
        "age_rating": attrs.get("ageRating"),
        "source_url": source_url,
        "scraped_at": now_text(),
    }


def build_page_url(endpoint, page, limit=PAGE_LIMIT):
    """Add the page window to an endpoint, keeping any filters already in it.
    Page 1 is offset 0, page 2 is offset 20, and so on."""
    parts = urlparse(endpoint)
    query = dict(parse_qsl(parts.query))
    query["page[limit]"] = str(limit)
    query["page[offset]"] = str((page - 1) * limit)
    return urlunparse(parts._replace(query=urlencode(query)))


def resolve_url(user_url):
    """Understand a URL typed into the bonus box.

    Returns ("single", api_url) or ("list", api_url). Raises ValueError with a
    clear message for anything else.
    """
    text = (user_url or "").strip()
    if not text:
        raise ValueError("Please paste a URL first.")
    if "://" not in text:
        text = "https://" + text
    parts = urlparse(text)
    host = (parts.netloc or "").lower()
    path = parts.path or ""
    if host.startswith("www."):
        host = host[4:]

    if host in ("kitsu.io", "kitsu.app"):
        # Website page: https://kitsu.app/anime/cowboy-bebop   or  /anime/1
        m = re.match(r"^/anime/([^/]+)", path)
        if m:
            key = m.group(1)
            if key.isdigit():
                return "single", "{}/anime/{}".format(API_BASE, key)
            return "single", "{}/anime?{}".format(
                API_BASE, urlencode({"filter[slug]": key}))
        # API address: https://kitsu.io/api/edge/anime/1   or   .../anime?filter[...]
        m = re.match(r"^/api/edge/anime/(\d+)/?$", path)
        if m:
            return "single", "{}/anime/{}".format(API_BASE, m.group(1))
        if re.match(r"^/api/edge/anime/?$", path):
            return "list", "{}/anime{}".format(
                API_BASE, ("?" + parts.query) if parts.query else "")
        raise ValueError("That Kitsu address is not an anime. Paste an anime page "
                         "(for example https://kitsu.app/anime/cowboy-bebop).")

    raise ValueError("Unsupported URL. Paste a Kitsu anime page "
                     "(kitsu.app/anime/...) or a Kitsu API address (kitsu.io/api/edge/anime).")


class Scraper:
    """Runs the scraping loop in its own thread.

    Callbacks (all optional, all called from the worker thread):
        on_progress(saved_count, target)
        on_log(text)
        on_error(text)
        on_done(status, reason)
    """

    def __init__(self, db, target=15000, endpoint=ANIME_LIST_URL, state_key="last_page",
                 delay=REQUEST_DELAY, retry_waits=RETRY_WAITS, session=None,
                 on_progress=None, on_log=None, on_error=None, on_done=None,
                 log_path=LOG_PATH):
        self.db = db
        self.target = target
        self.endpoint = endpoint
        self.state_key = state_key
        self.delay = delay
        self.retry_waits = retry_waits
        self.session = session or requests.Session()
        self.session.headers.update(HEADERS)
        self.on_progress = on_progress
        self.on_log = on_log
        self.on_error = on_error
        self.on_done = on_done
        self.log_path = log_path

        self.status = STATUS_IDLE
        self._run_flag = threading.Event()     # set = running, clear = paused
        self._stop_flag = threading.Event()
        self._thread = None

    # ---- controls (called from the UI) -------------------------------
    def start(self, restart=False):
        """Start from the saved page. restart=True begins again at page 1."""
        if self._thread and self._thread.is_alive():
            return
        if restart:
            self.db.reset_progress(self.state_key)
        self._stop_flag.clear()
        self._run_flag.set()
        self.status = STATUS_RUNNING
        self._thread = threading.Thread(target=self._run, name="scraper", daemon=True)
        self._thread.start()

    def pause(self):
        if self.status == STATUS_RUNNING:
            self._run_flag.clear()
            self.status = STATUS_PAUSED
            self._log("Paused.")

    def resume(self):
        if self.status == STATUS_PAUSED:
            self.status = STATUS_RUNNING
            self._run_flag.set()
            self._log("Resumed.")

    def stop(self):
        if self.status in (STATUS_RUNNING, STATUS_PAUSED):
            self._stop_flag.set()
            self._run_flag.set()               # wake a paused loop so it can exit
            self._log("Stop requested.")

    def join(self, timeout=None):
        if self._thread:
            self._thread.join(timeout)

    def is_active(self):
        return bool(self._thread and self._thread.is_alive())

    # ---- helpers -------------------------------------------------------
    def _log(self, text):
        line = "{} | {}".format(now_text(), text)
        try:
            os.makedirs(os.path.dirname(self.log_path), exist_ok=True)
            with open(self.log_path, "a", encoding="utf-8") as f:
                f.write(line + "\n")
        except OSError:
            pass                                # a log problem must never stop scraping
        self._call(self.on_log, line)

    def _call(self, fn, *args):
        if fn is None:
            return
        try:
            fn(*args)
        except Exception:
            pass                                # a UI callback problem must not kill the thread

    def _wait(self, seconds):
        """Sleep that wakes up immediately when Stop is pressed.
        Returns True if the full time passed, False if stop was requested."""
        return not self._stop_flag.wait(seconds)

    def _fetch(self, url):
        """GET one URL with retries. Returns the parsed JSON dict or None."""
        last_problem = "unknown problem"
        for attempt in range(len(self.retry_waits) + 1):
            if self._stop_flag.is_set():
                return None
            wait = self.retry_waits[attempt] if attempt < len(self.retry_waits) else None
            try:
                r = self.session.get(url, timeout=30)
                if r.status_code == 200:
                    try:
                        return r.json()
                    except ValueError:
                        last_problem = "answer was not valid JSON"
                elif r.status_code in NO_RETRY_CODES:
                    self._log("{} -> HTTP {} (not retrying)".format(url, r.status_code))
                    return None
                else:
                    last_problem = "HTTP {}".format(r.status_code)
                    if r.status_code == 429:
                        try:
                            wait = max(wait or 0, int(r.headers.get("Retry-After", 0)))
                        except (TypeError, ValueError):
                            pass
            except requests.RequestException as e:
                last_problem = type(e).__name__
            if wait is None:
                break
            self._log("{} -> {} (retry {}/{} in {}s)".format(
                url, last_problem, attempt + 1, len(self.retry_waits), wait))
            if not self._wait(wait):
                return None
        self._log("{} -> giving up: {}".format(url, last_problem))
        return None

    # ---- the loop -----------------------------------------------------
    def _run(self):
        status, reason = STATUS_FINISHED, "Target reached."
        try:
            page = self.db.get_saved_page(self.state_key) + 1
            self._log("Started at page {} ({} records saved).".format(page, self.db.count()))
            self._call(self.on_progress, self.db.count(), self.target)

            while self.db.count() < self.target:
                self._run_flag.wait()                       # blocks while paused
                if self._stop_flag.is_set():
                    status, reason = STATUS_STOPPED, "Stopped by user."
                    break

                url = build_page_url(self.endpoint, page)
                body = self._fetch(url)
                if self._stop_flag.is_set():
                    status, reason = STATUS_STOPPED, "Stopped by user."
                    break
                if body is None:
                    status = STATUS_FAILED
                    reason = ("Could not get page {}. Saved data is safe and will be used. "
                              "Press Start to try again.".format(page))
                    self._call(self.on_error, reason)
                    break

                items = body.get("data") or []
                rows = [r for r in (parse_anime(i, url) for i in items) if r]
                added = self.db.save_page(page, rows, self.state_key)
                total = self.db.count()
                self._log("page {} | {} records | {} new | total {}".format(
                    page, len(rows), added, total))
                self._call(self.on_progress, total, self.target)

                links = body.get("links")
                has_next = bool(links.get("next")) if isinstance(links, dict) else bool(items)
                if not items or not has_next:
                    status, reason = STATUS_FINISHED, "No more pages on the website."
                    break
                page += 1
                if not self._wait(self.delay):
                    status, reason = STATUS_STOPPED, "Stopped by user."
                    break
        except Exception as e:                              # nothing escapes this thread
            status = STATUS_FAILED
            reason = "Scraper error: {}. Saved data is safe and will be used.".format(e)
            self._log(reason)
            self._call(self.on_error, reason)

        self.status = status
        self._log("Finished with status '{}': {}".format(status, reason))
        self._call(self.on_done, status, reason)


def scrape_single(db, api_url, session=None):
    """Fetch ONE anime (bonus URL box). Returns (ok, message)."""
    session = session or requests.Session()
    session.headers.update(HEADERS)
    try:
        r = session.get(api_url, timeout=30)
        if r.status_code == 404:
            return False, "That anime was not found."
        if r.status_code != 200:
            return False, "Website answered HTTP {}. Try again in a minute.".format(r.status_code)
        data = (r.json() or {}).get("data")
        if isinstance(data, list):                 # a search by slug returns a list
            data = data[0] if data else None
        row = parse_anime(data, api_url)
        if not row:
            return False, "The website had no anime for that address."
        added = db.save_page(None, [row])
        msg = ("Added '{}'.".format(row["title"]) if added
               else "'{}' was already in the database.".format(row["title"]))
        return True, msg
    except requests.RequestException as e:
        return False, "Network problem: {}. Saved data is still available.".format(type(e).__name__)
    except Exception as e:
        return False, "Unexpected problem: {}.".format(e)
