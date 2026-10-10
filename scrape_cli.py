"""Scrape anime from the terminal (no window needed) and save to data/anime.db.

Run:      python scrape_cli.py               (default target 15000)
          python scrape_cli.py 200           (small test, 200 records)
Keys:     Ctrl+C = stop safely. Run the same command again to resume.
Then:     python scrape_cli.py --backup      (also write data/anime.json)
"""
import sys
import time

from core.database import Database
from core.scraper import Scraper


def main():
    db = Database()
    if "--backup" in sys.argv:
        print("Saved", db.export_json(), "records to data/anime.json")
        return

    nums = [a for a in sys.argv[1:] if a.isdigit()]
    target = int(nums[0]) if nums else 15000
    print("Records already saved:", db.count(), "| target:", target)

    def progress(done, goal):
        pct = 100 * done / goal if goal else 0
        print("\r{:,} / {:,} ({:.0f}%)".format(done, goal, pct), end="", flush=True)

    scraper = Scraper(db, target=target, on_progress=progress,
                      on_error=lambda m: print("\n!!", m))
    scraper.start()
    try:
        while scraper.is_active():
            time.sleep(0.3)
    except KeyboardInterrupt:
        print("\nStopping safely...")
        scraper.stop()
        scraper.join(30)
    print("\nStatus:", scraper.status, "| records in database:", db.count())
    if db.count():
        print("Backup written:", db.export_json(), "records -> data/anime.json")
    print("Details are in data/scrape_log.txt")


if __name__ == "__main__":
    main()
