"""
Qualification orchestration module.
Reads from raw_indeed_jobs, scores with LLM, sources HR contacts, inserts into qualified_indeed_jobs.
"""

import os
import logging
import time
from typing import Dict, Any, List
from datetime import datetime, timedelta

from .scoring import JobScorer
from .hr_contacts import HRContactSourcer
from .ddl import DatabaseManager

logger = logging.getLogger(__name__)


class JobQualifier:
    """Orchestrates job qualification pipeline: score → contact → insert."""

    def __init__(self):
        """Initialize qualifier with components."""
        self.scorer = JobScorer()
        self.hr_sourcer = HRContactSourcer()
        self.db_manager = None
        self.threshold = int(os.getenv('QUALIFY_SCORE_THRESHOLD', '80'))
        self.rate_limit_sleep = float(os.getenv('RATE_LIMIT_SLEEP_SEC', '0.5'))

    def fetch_raw_jobs(self, from_hours: int = 24) -> List[Dict[str, Any]]:
        """
        Fetch raw jobs from last N hours.

        Args:
            from_hours: Hours back to fetch

        Returns:
            List of job dictionaries
        """
        try:
            if self.db_manager.engine == 'postgres':
                sql = """
                    SELECT
                        id, job_key, job_hash, title, company_name, location_fmt_short,
                        date_published, salary_text, job_url, apply_url,
                        description_text, description_html, job_types, attributes,
                        shift_and_schedule, is_remote
                    FROM raw_indeed_jobs
                    WHERE date_published >= (NOW() AT TIME ZONE 'UTC') - INTERVAL '%s hours'
                    ORDER BY date_published DESC
                """ % from_hours
            elif self.db_manager.engine == 'mysql':
                sql = """
                    SELECT
                        id, job_key, job_hash, title, company_name, location_fmt_short,
                        date_published, salary_text, job_url, apply_url,
                        description_text, description_html, job_types, attributes,
                        shift_and_schedule, is_remote
                    FROM raw_indeed_jobs
                    WHERE date_published >= (UTC_TIMESTAMP() - INTERVAL %s HOUR)
                    ORDER BY date_published DESC
                """ % from_hours
            else:
                return []

            self.db_manager.cursor.execute(sql)
            rows = self.db_manager.cursor.fetchall()

            # Convert to dicts
            columns = ['id', 'job_key', 'job_hash', 'title', 'company_name', 'location_fmt_short',
                      'date_published', 'salary_text', 'job_url', 'apply_url',
                      'description_text', 'description_html', 'job_types', 'attributes',
                      'shift_and_schedule', 'is_remote']

            jobs = []
            for row in rows:
                job = dict(zip(columns, row))
                jobs.append(job)

            logger.info(f"Fetched {len(jobs)} raw jobs from last {from_hours} hours")
            return jobs

        except Exception as e:
            logger.error(f"Error fetching raw jobs: {e}")
            return []

    def qualify_job(self, job: Dict[str, Any]) -> Dict[str, Any]:
        """
        Qualify a single job: score → contact → build qualified record.

        Args:
            job: Raw job dictionary

        Returns:
            Qualified job dictionary or None if rejected
        """
        job_title = job.get('title', 'Unknown')
        company = job.get('company_name', 'Unknown')

        logger.info(f"Qualifying: {job_title} at {company}")

        # 1. Score job
        score_result = self.scorer.score_job_with_retry(job, max_retries=1)

        if not score_result:
            logger.warning(f"Failed to score job: {job_title}")
            return None

        # 2. Check if recommended
        if not score_result.get('recommended', False):
            logger.info(f"Job not recommended (score={score_result.get('score', 0)}): {job_title}")
            return None

        # 3. Check hard fail (citizenship)
        if score_result.get('requires_us_citizenship', False):
            logger.info(f"Hard fail - requires citizenship: {job_title}")
            return None

        # 4. Get company 30d count
        company_30d_count = self.db_manager.get_company_30d_count(company)

        # 5. Source HR contact (best effort)
        hr_contact = None
        if os.getenv('APOLLO_IO_API_KEY'):
            try:
                hr_contact = self.hr_sourcer.find_best_hr_contact(
                    company_name=company,
                    job_title=job_title,
                    location=job.get('location_fmt_short', '')
                )
                time.sleep(self.rate_limit_sleep)
            except Exception as e:
                logger.error(f"Error sourcing HR contact: {e}")

        # 6. Build qualified job record
        qualified = {
            'raw_job_id': job.get('id'),
            'job_hash': job.get('job_hash'),
            'job_key': job.get('job_key'),
            'title': job.get('title'),
            'company_name': company,
            'location_fmt_short': job.get('location_fmt_short'),
            'date_published': job.get('date_published'),
            'salary_text': job.get('salary_text'),
            'job_url': job.get('job_url'),
            'apply_url': job.get('apply_url'),
            'description_text': job.get('description_text'),
            'description_html': job.get('description_html'),
            'score': score_result.get('score'),
            'reasons': score_result.get('reasons', []),
            'flags': {
                'requires_us_citizenship': score_result.get('requires_us_citizenship', False),
                'is_packaging_or_operator_role': score_result.get('is_packaging_or_operator_role', False),
                'matched_keywords': score_result.get('matched_keywords', []),
                'red_flags': score_result.get('red_flags', []),
                'confidence': score_result.get('confidence', 0.0)
            },
            'company_30d_postings_count': company_30d_count
        }

        # Add HR contact if found
        if hr_contact:
            qualified['hr_contact_name'] = hr_contact.get('name')
            qualified['hr_contact_title'] = hr_contact.get('title')
            qualified['hr_contact_email'] = hr_contact.get('email')
            qualified['hr_contact_linkedin'] = hr_contact.get('linkedin')
            logger.info(f"HR contact found: {hr_contact.get('name')}")
        else:
            logger.info("No HR contact found")

        logger.info(f"Qualified job: {job_title} (score={qualified['score']}, "
                   f"company_30d={company_30d_count}, hr_contact={bool(hr_contact)})")

        return qualified

    def run_nightly(self) -> Dict[str, int]:
        """
        Run nightly qualification: last 24 hours.

        Returns:
            Stats dictionary with counts
        """
        logger.info("=== NIGHTLY QUALIFICATION MODE ===")
        logger.info("Processing jobs from last 24 hours")

        return self.run_qualification(from_hours=24)

    def run_backfill(self, days: int) -> Dict[str, int]:
        """
        Run backfill qualification: last N days.

        Args:
            days: Number of days to backfill

        Returns:
            Stats dictionary with counts
        """
        logger.info(f"=== BACKFILL QUALIFICATION MODE ===")
        logger.info(f"Processing jobs from last {days} days")

        return self.run_qualification(from_hours=days * 24)

    def run_qualification(self, from_hours: int) -> Dict[str, int]:
        """
        Run qualification pipeline.

        Args:
            from_hours: Hours back to process

        Returns:
            Stats dictionary
        """
        stats = {
            'fetched': 0,
            'scored': 0,
            'passed': 0,
            'failed': 0,
            'hard_fail_citizenship': 0,
            'inserted': 0,
            'json_errors': 0,
            'contact_found': 0
        }

        # Connect to database
        logger.info("Connecting to database...")
        self.db_manager = DatabaseManager()
        self.db_manager.connect()

        try:
            # Fetch raw jobs
            logger.info(f"Fetching jobs from last {from_hours} hours...")
            raw_jobs = self.fetch_raw_jobs(from_hours=from_hours)
            stats['fetched'] = len(raw_jobs)

            if not raw_jobs:
                logger.info("No jobs to process")
                return stats

            # Process each job
            for job in raw_jobs:
                try:
                    # Qualify job
                    qualified = self.qualify_job(job)

                    if qualified:
                        stats['passed'] += 1

                        # Track HR contact
                        if qualified.get('hr_contact_name'):
                            stats['contact_found'] += 1

                        # Insert into qualified_indeed_jobs
                        success = self.db_manager.upsert_qualified_job(qualified)

                        if success:
                            stats['inserted'] += 1
                            logger.info(f"Inserted qualified job: {qualified.get('title')}")
                    else:
                        stats['failed'] += 1

                    stats['scored'] += 1

                except Exception as e:
                    logger.error(f"Error qualifying job {job.get('title')}: {e}")
                    stats['json_errors'] += 1

            # Log summary
            logger.info(f"\n=== QUALIFICATION SUMMARY ===")
            logger.info(f"Fetched: {stats['fetched']}")
            logger.info(f"Scored: {stats['scored']}")
            logger.info(f"Passed (score >= {self.threshold}): {stats['passed']}")
            logger.info(f"Failed (score < {self.threshold}): {stats['failed']}")
            logger.info(f"Inserted into qualified_indeed_jobs: {stats['inserted']}")
            logger.info(f"HR contacts found: {stats['contact_found']}")
            logger.info(f"JSON/scoring errors: {stats['json_errors']}")

            return stats

        finally:
            self.db_manager.disconnect()
