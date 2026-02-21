"""Run the Bug Civil Court demo scraper using the PersistentDriver.

Requires the demo website to be running (``python -m kent.demo.run_demo``).
"""

from __future__ import annotations

import argparse
import asyncio
from pathlib import Path

from kent.demo.scraper import BugCourtDemoScraper
from kent.driver.persistent_driver import PersistentDriver


async def run(args: argparse.Namespace) -> None:
    scraper = BugCourtDemoScraper()

    async with PersistentDriver.open(
        scraper,
        db_path=Path(args.db_path),
        num_workers=args.num_workers,
        max_workers=args.max_workers,
        resume=args.resume,
        storage_dir=Path(args.storage_dir) if args.storage_dir else None,
    ) as driver:

        async def _print_progress(event):
            print(event.to_json())

        driver.on_progress = _print_progress
        await driver.run()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run the Bug Civil Court demo scraper with PersistentDriver.",
    )
    parser.add_argument(
        "--db-path",
        default="demo_run.db",
        help="Path to the SQLite database file (default: demo_run.db)",
    )
    parser.add_argument(
        "--num-workers",
        type=int,
        default=1,
        help="Number of initial concurrent workers (default: 1)",
    )
    parser.add_argument(
        "--max-workers",
        type=int,
        default=10,
        help="Maximum workers for dynamic scaling (default: 10)",
    )
    parser.add_argument(
        "--no-resume",
        dest="resume",
        action="store_false",
        help="Start fresh instead of resuming from existing queue state",
    )
    parser.add_argument(
        "--storage-dir",
        default=None,
        help="Directory for downloaded files (default: auto)",
    )
    asyncio.run(run(parser.parse_args()))


if __name__ == "__main__":
    main()
