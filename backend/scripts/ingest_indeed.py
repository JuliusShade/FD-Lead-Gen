#!/usr/bin/env python3
"""
Indeed job ingestion CLI.

Modes:
  discover  - Perform schema discovery and create table
  backfill  - Historical data load (30 days default)
  nightly   - Last 24 hours (default for scheduled runs)
  custom    - Custom parameters

Examples:
  python scripts/ingest_indeed.py --mode discover
  python scripts/ingest_indeed.py --mode backfill --fromDays 30 --maxPages 10
  python scripts/ingest_indeed.py --mode nightly
  python scripts/ingest_indeed.py --mode custom --query "developer" --location "San Francisco" --fromDays 7 --maxPages 5
"""

import os
import sys
import argparse
import logging
from pathlib import Path
from dotenv import load_dotenv

# Add src to path for imports
backend_dir = Path(__file__).parent.parent
sys.path.insert(0, str(backend_dir / 'src'))

from indeed.ingest import IndeedIngestionOrchestrator


def setup_logging(verbose: bool = False):
    """
    Configure logging.

    Args:
        verbose: Enable debug logging
    """
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )


def main():
    """Main CLI entrypoint."""
    parser = argparse.ArgumentParser(
        description='Indeed job ingestion via RapidAPI',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )

    # Mode selection
    parser.add_argument(
        '--mode',
        required=True,
        choices=['discover', 'backfill', 'nightly', 'custom'],
        help='Ingestion mode'
    )

    # Search parameters
    parser.add_argument(
        '--query',
        default='packaging',
        help='Job search query (default: packaging)'
    )

    parser.add_argument(
        '--location',
        default='Springfield, OH',
        help='Location string (default: Springfield, OH)'
    )

    # Temporal parameters
    parser.add_argument(
        '--fromDays',
        type=int,
        default=None,
        help='Days back to search (default: 30 for backfill, 1 for nightly)'
    )

    # Pagination parameters
    parser.add_argument(
        '--maxPages',
        type=int,
        default=None,
        help='Maximum pages to fetch (default: 10)'
    )

    parser.add_argument(
        '--pageSize',
        type=int,
        default=None,
        help='Results per page (default: 15, from env PAGE_SIZE)'
    )

    # Discovery parameters
    parser.add_argument(
        '--sampleSize',
        type=int,
        default=50,
        help='Sample size for schema discovery (default: 50)'
    )

    # Logging
    parser.add_argument(
        '--verbose',
        action='store_true',
        help='Enable verbose debug logging'
    )

    args = parser.parse_args()

    # Load environment variables
    env_file = backend_dir / '.env'
    if env_file.exists():
        load_dotenv(env_file)
    else:
        print(f"Warning: .env file not found at {env_file}")
        print("Using environment variables from system")

    # Override env vars if CLI args provided
    if args.pageSize is not None:
        os.environ['PAGE_SIZE'] = str(args.pageSize)

    if args.maxPages is not None:
        os.environ['MAX_PAGES'] = str(args.maxPages)

    # Setup logging
    setup_logging(args.verbose)
    logger = logging.getLogger(__name__)

    logger.info(f"Starting Indeed ingestion: mode={args.mode}")

    # Create orchestrator
    orchestrator = IndeedIngestionOrchestrator()

    try:
        # Run selected mode
        if args.mode == 'discover':
            result = orchestrator.discover_and_create_table(
                query=args.query,
                location=args.location,
                sample_size=args.sampleSize
            )

        elif args.mode == 'backfill':
            from_days = args.fromDays if args.fromDays is not None else 30
            max_pages = args.maxPages if args.maxPages is not None else 10

            result = orchestrator.run_backfill(
                query=args.query,
                location=args.location,
                from_days=from_days,
                max_pages=max_pages
            )

        elif args.mode == 'nightly':
            result = orchestrator.run_nightly(
                query=args.query,
                location=args.location
            )

        elif args.mode == 'custom':
            if args.fromDays is None or args.maxPages is None:
                logger.error("--fromDays and --maxPages are required for custom mode")
                sys.exit(1)

            result = orchestrator.run_custom(
                query=args.query,
                location=args.location,
                from_days=args.fromDays,
                max_pages=args.maxPages
            )

        else:
            logger.error(f"Unknown mode: {args.mode}")
            sys.exit(1)

        # Print summary
        logger.info("=== SUMMARY ===")
        for key, value in result.items():
            logger.info(f"  {key}: {value}")

        if result.get('success', False):
            logger.info("Ingestion completed successfully!")
            sys.exit(0)
        else:
            logger.error("Ingestion failed")
            sys.exit(1)

    except KeyboardInterrupt:
        logger.warning("Interrupted by user")
        sys.exit(130)

    except Exception as e:
        logger.exception(f"Fatal error: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()
