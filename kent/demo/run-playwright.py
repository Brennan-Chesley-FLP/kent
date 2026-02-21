"""Run the Bug Civil Court demo scraper using the PlaywrightDriver.

Requires the demo website to be running (``python -m kent.demo.run_demo``).
"""

from __future__ import annotations

import argparse
import asyncio
from pathlib import Path

from kent.demo.scraper import BugCourtDemoScraper
from kent.driver.playwright_driver import PlaywrightDriver


async def run(args: argparse.Namespace) -> None:
    scraper = BugCourtDemoScraper()

    viewport = {"width": args.viewport_width, "height": args.viewport_height}

    async with PlaywrightDriver.open(
        scraper,
        db_path=Path(args.db_path),
        browser_type=args.browser_type,
        headless=args.headless,
        viewport=viewport,
        locale=args.locale,
        timezone_id=args.timezone_id,
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
        description="Run the Bug Civil Court demo scraper with PlaywrightDriver.",
    )
    parser.add_argument(
        "--db-path",
        default="demo_run.db",
        help="Path to the SQLite database file (default: demo_run.db)",
    )
    parser.add_argument(
        "--browser-type",
        choices=["chromium", "firefox", "webkit"],
        default="chromium",
        help="Browser engine to use (default: chromium)",
    )
    parser.add_argument(
        "--no-headless",
        dest="headless",
        action="store_false",
        help="Show the browser window (default: headless)",
    )
    parser.add_argument(
        "--viewport-width",
        type=int,
        default=1280,
        help="Browser viewport width (default: 1280)",
    )
    parser.add_argument(
        "--viewport-height",
        type=int,
        default=720,
        help="Browser viewport height (default: 720)",
    )
    parser.add_argument(
        "--locale",
        default="en-US",
        help="Browser locale (default: en-US)",
    )
    parser.add_argument(
        "--timezone-id",
        default="America/New_York",
        help="Browser timezone (default: America/New_York)",
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
