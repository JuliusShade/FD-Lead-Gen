#!/usr/bin/env python3
"""
CLI entrypoint for Indeed job qualification.
Modes: nightly (last 24h), backfill (last N days).
"""

import os
import sys
import argparse
import logging
from pathlib import Path
from dotenv import load_dotenv

# Add src to path
backend_dir = Path(__file__).parent.parent
sys.path.insert(0, str(backend_dir))

# Load environment
load_dotenv(backend_dir / '.env')

from src.indeed.qualify import JobQualifier
from src.indeed.ddl import DatabaseManager


def setup_logging(verbose: bool = False):
    """Configure logging."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )


def create_table_if_needed():
    """Create qualified_indeed_jobs table if it doesn't exist."""
    db = DatabaseManager()
    db.connect()
    try:
        db.create_qualified_jobs_table()
        logging.info("Qualified jobs table ready")
    except Exception as e:
        logging.warning(f"Table may already exist: {e}")
    finally:
        db.disconnect()


def main():
    """Main CLI entrypoint."""
    parser = argparse.ArgumentParser(
        description='Qualify Indeed jobs with LLM scoring and HR contact sourcing'
    )

    parser.add_argument(
        '--mode',
        type=str,
        required=True,
        choices=['nightly', 'backfill', 'setup'],
        help='Qualification mode: nightly (last 24h), backfill (last N days), setup (create table)'
    )

    parser.add_argument(
        '--backfillDays',
        type=int,
        default=30,
        help='Days to backfill (for backfill mode)'
    )

    parser.add_argument(
        '--threshold',
        type=int,
        help='Score threshold for qualification (overrides QUALIFY_SCORE_THRESHOLD env var)'
    )

    parser.add_argument(
        '--verbose',
        action='store_true',
        help='Enable debug logging'
    )

    args = parser.parse_args()

    # Setup logging
    setup_logging(args.verbose)

    # Override threshold if provided
    if args.threshold:
        os.environ['QUALIFY_SCORE_THRESHOLD'] = str(args.threshold)

    # Validate API keys
    if args.mode != 'setup':
        if not os.getenv('OPENAI_API_KEY'):
            logging.error("OPENAI_API_KEY not set in .env")
            sys.exit(1)

        logging.info(f"Using OpenAI model: {os.getenv('OPENAI_MODEL', 'gpt-4o-mini')}")
        logging.info(f"Score threshold: {os.getenv('QUALIFY_SCORE_THRESHOLD', '80')}")

        if os.getenv('APOLLO_IO_API_KEY'):
            logging.info("Apollo.io API key found - HR contact sourcing enabled")
        else:
            logging.warning("APOLLO_IO_API_KEY not set - HR contact sourcing disabled")

    # Execute mode
    if args.mode == 'setup':
        logging.info("=== SETUP MODE ===")
        create_table_if_needed()
        logging.info("Setup complete!")

    elif args.mode == 'nightly':
        qualifier = JobQualifier()
        stats = qualifier.run_nightly()

        # Exit code based on results
        if stats['json_errors'] > 0:
            logging.warning(f"Completed with {stats['json_errors']} errors")
            sys.exit(1)
        else:
            logging.info("Nightly qualification complete!")
            sys.exit(0)

    elif args.mode == 'backfill':
        logging.info(f"Backfilling last {args.backfillDays} days...")

        qualifier = JobQualifier()
        stats = qualifier.run_backfill(days=args.backfillDays)

        # Exit code based on results
        if stats['json_errors'] > 0:
            logging.warning(f"Completed with {stats['json_errors']} errors")
            sys.exit(1)
        else:
            logging.info("Backfill qualification complete!")
            sys.exit(0)


if __name__ == '__main__':
    main()
