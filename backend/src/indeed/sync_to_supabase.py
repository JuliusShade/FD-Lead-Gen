#!/usr/bin/env python3
"""
Sync qualified_indeed_jobs from RDS PostgreSQL to Supabase.

This module transforms qualified jobs into the JobPostingSummary schema
that the frontend expects and syncs them to Supabase using REST API.
"""

import os
import sys
from typing import List, Dict, Any
from datetime import datetime
import logging
import json

# Add site-packages to path
site_packages = r'C:\Python311\Lib\site-packages'
if site_packages not in sys.path:
    sys.path.insert(0, site_packages)

try:
    import requests
    import psycopg2
    from psycopg2.extras import RealDictCursor
    from dotenv import load_dotenv
except ImportError as e:
    print(f"Missing required package: {e}")
    print("Install with: pip install requests psycopg2-binary python-dotenv")
    sys.exit(1)

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


class SupabaseSync:
    """Sync qualified jobs from RDS to Supabase using REST API."""

    def __init__(self):
        """Initialize database connections."""
        # RDS PostgreSQL connection
        self.rds_conn = None

        # Supabase REST API configuration
        self.supabase_url = os.getenv('SUPABASE_URL')
        self.supabase_key = os.getenv('SUPABASE_SERVICE_ROLE_KEY')

        if not self.supabase_url or not self.supabase_key:
            raise ValueError("SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY must be set in .env")

        # REST API endpoint
        self.api_url = f"{self.supabase_url}/rest/v1"
        self.headers = {
            'apikey': self.supabase_key,
            'Authorization': f'Bearer {self.supabase_key}',
            'Content-Type': 'application/json',
            'Prefer': 'return=representation'
        }

    def connect_rds(self):
        """Connect to RDS PostgreSQL."""
        try:
            self.rds_conn = psycopg2.connect(
                host=os.getenv('DB_HOST'),
                port=int(os.getenv('DB_PORT', '5432')),
                database=os.getenv('DB_DATABASE'),
                user=os.getenv('DB_USERNAME'),
                password=os.getenv('DB_PASSWORD'),
                sslmode='require' if os.getenv('DB_SSL', 'true').lower() == 'true' else 'prefer'
            )
            logger.info(f"Connected to RDS: {os.getenv('DB_HOST')}")
        except Exception as e:
            logger.error(f"Failed to connect to RDS: {e}")
            raise

    def fetch_qualified_jobs(self, limit: int = None) -> List[Dict[str, Any]]:
        """
        Fetch qualified jobs from RDS PostgreSQL.

        Args:
            limit: Optional limit on number of jobs to fetch

        Returns:
            List of qualified job records as dictionaries
        """
        if not self.rds_conn:
            self.connect_rds()

        cursor = self.rds_conn.cursor(cursor_factory=RealDictCursor)

        sql = """
            SELECT
                id,
                job_hash,
                title,
                company_name,
                location_fmt_short,
                date_published,
                salary_text,
                job_url,
                apply_url,
                description_text,
                description_html,
                hr_contact_name,
                hr_contact_title,
                hr_contact_email,
                hr_contact_linkedin,
                score,
                reasons,
                flags,
                company_30d_postings_count,
                populated_at
            FROM qualified_indeed_jobs
            ORDER BY score DESC, populated_at DESC
        """

        if limit:
            sql += f" LIMIT {limit}"

        cursor.execute(sql)
        jobs = cursor.fetchall()
        cursor.close()

        logger.info(f"Fetched {len(jobs)} qualified jobs from RDS")
        return jobs

    def transform_to_job_posting_summary(self, job: Dict[str, Any]) -> Dict[str, Any]:
        """
        Transform qualified job to JobPostingSummary schema.

        Frontend expects:
        - id: number
        - company: string
        - position: string | null
        - reason: string | null
        - ai_rationale: string | null
        - type_of_role: string[] | null
        - number_of_positions_last_30: string | null
        - estimated_opportunity: string | null
        - primary_contact: string | null
        - created_at: string (ISO date)
        - date_processed: string (ISO date)

        Args:
            job: Qualified job record from RDS

        Returns:
            Transformed job posting summary
        """
        # Extract reasons from JSONB array
        reasons_list = job.get('reasons') or []
        reason_text = "; ".join(reasons_list) if reasons_list else None

        # Extract AI rationale (first reason or summary)
        ai_rationale = reasons_list[0] if reasons_list else None

        # Extract type_of_role from flags or description
        flags = job.get('flags') or {}
        matched_keywords = flags.get('matched_keywords', [])
        type_of_role = matched_keywords if matched_keywords else None

        # Build primary contact string
        hr_name = job.get('hr_contact_name')
        hr_title = job.get('hr_contact_title')
        hr_email = job.get('hr_contact_email')
        hr_linkedin = job.get('hr_contact_linkedin')

        primary_contact = None
        if hr_name:
            contact_parts = [hr_name]
            if hr_title:
                contact_parts.append(f"({hr_title})")
            if hr_email:
                contact_parts.append(f"| {hr_email}")
            if hr_linkedin:
                contact_parts.append(f"| {hr_linkedin}")
            primary_contact = " ".join(contact_parts)

        # Estimated opportunity (using salary or score as proxy)
        estimated_opportunity = job.get('salary_text')
        if not estimated_opportunity:
            # Use score as quality indicator
            score = job.get('score', 0)
            if score >= 90:
                estimated_opportunity = "High potential"
            elif score >= 85:
                estimated_opportunity = "Good fit"
            else:
                estimated_opportunity = "Qualified"

        return {
            # Use job_hash as unique identifier for upsert
            'job_hash': job.get('job_hash'),
            'company': job.get('company_name'),
            'position': job.get('title'),
            'reason': reason_text,
            'ai_rationale': ai_rationale,
            'type_of_role': type_of_role,
            'number_of_positions_last_30': str(job.get('company_30d_postings_count', 0)),
            'estimated_opportunity': estimated_opportunity,
            'primary_contact': primary_contact,
            'created_at': job.get('date_published').isoformat() if job.get('date_published') else None,
            'date_processed': job.get('populated_at').isoformat() if job.get('populated_at') else None,
            # Additional fields for reference
            'job_url': job.get('job_url'),
            'score': job.get('score'),
            'location': job.get('location_fmt_short'),
        }

    def sync_to_supabase(self, jobs: List[Dict[str, Any]], table_name: str = 'job_posting_summary'):
        """
        Sync transformed jobs to Supabase using REST API.

        Args:
            jobs: List of transformed job posting summaries
            table_name: Supabase table name
        """
        if not jobs:
            logger.warning("No jobs to sync")
            return

        try:
            # Upsert jobs to Supabase using REST API
            # Note: We'll use POST with resolution=merge-duplicates for upsert behavior
            url = f"{self.api_url}/{table_name}"

            # For upsert, we need to use resolution parameter
            headers = self.headers.copy()
            headers['Prefer'] = 'resolution=merge-duplicates,return=representation'

            response = requests.post(
                url,
                headers=headers,
                json=jobs
            )

            if response.status_code in [200, 201]:
                logger.info(f"✓ Synced {len(jobs)} jobs to Supabase table '{table_name}'")
                logger.debug(f"Response: {response.json()}")
            else:
                logger.error(f"Failed to sync: HTTP {response.status_code}")
                logger.error(f"Response: {response.text}")
                raise Exception(f"Supabase sync failed: {response.text}")

        except Exception as e:
            logger.error(f"Failed to sync to Supabase: {e}")
            raise

    def run_sync(self, limit: int = None):
        """
        Main sync workflow.

        Args:
            limit: Optional limit on number of jobs to sync
        """
        logger.info("=== STARTING SUPABASE SYNC ===")

        try:
            # 1. Fetch qualified jobs from RDS
            rds_jobs = self.fetch_qualified_jobs(limit=limit)

            if not rds_jobs:
                logger.info("No qualified jobs found in RDS")
                return

            # 2. Transform to JobPostingSummary schema
            transformed_jobs = [
                self.transform_to_job_posting_summary(job)
                for job in rds_jobs
            ]

            # 3. Sync to Supabase
            self.sync_to_supabase(transformed_jobs)

            logger.info("=== SYNC COMPLETE ===")

        except Exception as e:
            logger.error(f"Sync failed: {e}")
            raise

        finally:
            if self.rds_conn:
                self.rds_conn.close()
                logger.info("RDS connection closed")


def main():
    """CLI entrypoint."""
    import argparse

    parser = argparse.ArgumentParser(description='Sync qualified jobs to Supabase')
    parser.add_argument('--limit', type=int, help='Limit number of jobs to sync')
    parser.add_argument('--table', type=str, default='job_posting_summary',
                       help='Supabase table name (default: job_posting_summary)')

    args = parser.parse_args()

    syncer = SupabaseSync()
    syncer.run_sync(limit=args.limit)


if __name__ == '__main__':
    main()
