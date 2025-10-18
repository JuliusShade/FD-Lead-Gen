"""
Orchestration module for Indeed job ingestion.
Coordinates discovery, table creation, and data loading.
"""

import logging
from typing import Dict, Any, List, Optional

from .api import IndeedAPIClient
from .discover import SchemaDiscoverer
from .ddl import DatabaseManager
from .normalize import normalize_job_record, flatten_job_fields

logger = logging.getLogger(__name__)


class IndeedIngestionOrchestrator:
    """Orchestrates the full ingestion pipeline."""

    def __init__(self):
        """Initialize orchestrator with components."""
        self.api_client = IndeedAPIClient()
        self.discoverer = SchemaDiscoverer()
        self.db_manager = None

    def discover_and_create_table(
        self,
        query: str = "packaging",
        location: str = "Springfield, OH",
        sample_size: int = 50
    ) -> Dict[str, Any]:
        """
        Discovery mode: Fetch sample data, infer schema, create table.

        Args:
            query: Job search query
            location: Location string
            sample_size: Number of records to analyze

        Returns:
            Dictionary with discovery results
        """
        logger.info("=== DISCOVERY MODE ===")
        logger.info(f"Query: '{query}', Location: '{location}', Sample size: {sample_size}")

        # Fetch sample data
        logger.info("Fetching sample jobs from API...")
        jobs = self.api_client.fetch_jobs(
            query=query,
            location=location,
            from_days=1,
            max_pages=1  # Just one page for discovery
        )

        if not jobs:
            logger.error("No jobs returned from API")
            return {'success': False, 'error': 'No jobs returned from API'}

        logger.info(f"Fetched {len(jobs)} sample jobs")

        # Flatten and normalize for schema discovery
        flattened_jobs = []
        for job in jobs[:sample_size]:
            flattened = flatten_job_fields(job)
            flattened_jobs.append(flattened)

        # Discover schema
        logger.info("Discovering schema from sample jobs...")
        discovered_schema = self.discoverer.discover_schema(flattened_jobs)

        # Merge with core fields
        core_fields = self.discoverer.get_core_fields()
        full_schema = {**core_fields, **discovered_schema}

        logger.info(f"Discovered schema with {len(full_schema)} fields")

        # Check if provider_id exists
        has_provider_id = any(
            job.get('provider_id') or job.get('id') or job.get('jobId')
            for job in jobs[:sample_size]
        )

        # Connect to database
        logger.info("Connecting to database...")
        self.db_manager = DatabaseManager()
        self.db_manager.connect()

        try:
            # Create table
            logger.info("Creating table raw_indeed_jobs...")
            self.db_manager.create_table(full_schema, has_provider_id)

            logger.info("Discovery complete!")
            return {
                'success': True,
                'sample_size': len(jobs),
                'schema_fields': len(full_schema),
                'has_provider_id': has_provider_id,
                'schema': full_schema
            }

        finally:
            self.db_manager.disconnect()

    def run_backfill(
        self,
        query: str = "packaging",
        location: str = "Springfield, OH",
        from_days: int = 30,
        max_pages: int = 10
    ) -> Dict[str, Any]:
        """
        Backfill mode: Fetch historical data and insert into database.

        Args:
            query: Job search query
            location: Location string
            from_days: Days back to fetch (30 = last month)
            max_pages: Maximum pages to fetch

        Returns:
            Dictionary with backfill results
        """
        logger.info("=== BACKFILL MODE ===")
        logger.info(f"Query: '{query}', Location: '{location}', "
                   f"From days: {from_days}, Max pages: {max_pages}")

        # Fetch jobs
        logger.info("Fetching jobs from API...")
        jobs = self.api_client.fetch_jobs(
            query=query,
            location=location,
            from_days=from_days,
            max_pages=max_pages
        )

        if not jobs:
            logger.warning("No jobs returned from API")
            return {'success': True, 'jobs_fetched': 0, 'inserted': 0, 'skipped': 0, 'errors': 0}

        logger.info(f"Fetched {len(jobs)} total jobs")

        # Normalize and enrich jobs
        logger.info("Normalizing job records...")
        normalized_jobs = []
        for job in jobs:
            # Flatten fields
            flattened = flatten_job_fields(job)

            # Add normalized fields (job_hash, provider_id)
            normalized = normalize_job_record(flattened)

            normalized_jobs.append(normalized)

        # Connect to database
        logger.info("Connecting to database...")
        self.db_manager = DatabaseManager()
        self.db_manager.connect()

        try:
            # Insert jobs
            logger.info("Inserting jobs into database...")
            stats = self.db_manager.insert_jobs(normalized_jobs)

            logger.info(f"Backfill complete! Inserted: {stats['inserted']}, "
                       f"Skipped: {stats['skipped']}, Errors: {stats['errors']}")

            return {
                'success': True,
                'jobs_fetched': len(jobs),
                **stats
            }

        finally:
            self.db_manager.disconnect()

    def run_nightly(
        self,
        query: str = "packaging",
        location: str = "Springfield, OH"
    ) -> Dict[str, Any]:
        """
        Nightly mode: Fetch last 24 hours of jobs.

        Args:
            query: Job search query
            location: Location string

        Returns:
            Dictionary with ingestion results
        """
        logger.info("=== NIGHTLY MODE ===")
        logger.info(f"Query: '{query}', Location: '{location}', From: last 24 hours")

        # Run with from_days=1 (last 24 hours)
        return self.run_backfill(
            query=query,
            location=location,
            from_days=1,
            max_pages=10  # Can adjust based on expected volume
        )

    def run_custom(
        self,
        query: str,
        location: str,
        from_days: int,
        max_pages: int
    ) -> Dict[str, Any]:
        """
        Custom mode: Run with fully custom parameters.

        Args:
            query: Job search query
            location: Location string
            from_days: Days back to fetch
            max_pages: Maximum pages to fetch

        Returns:
            Dictionary with ingestion results
        """
        logger.info("=== CUSTOM MODE ===")
        logger.info(f"Query: '{query}', Location: '{location}', "
                   f"From days: {from_days}, Max pages: {max_pages}")

        return self.run_backfill(
            query=query,
            location=location,
            from_days=from_days,
            max_pages=max_pages
        )
