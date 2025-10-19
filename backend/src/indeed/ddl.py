"""
DDL generation and execution for raw_indeed_jobs table.
Supports both PostgreSQL and MySQL.
"""

import os
import logging
import psycopg2
from psycopg2.extras import Json
from typing import Dict, Optional, Any, List
import json

logger = logging.getLogger(__name__)


class DatabaseManager:
    """Manages database connections and DDL operations."""

    def __init__(self):
        """Initialize database manager from environment config."""
        self.engine = os.getenv('DB_ENGINE', 'postgres').lower()
        self.host = os.getenv('DB_HOST')
        self.port = int(os.getenv('DB_PORT', '5432'))
        self.database = os.getenv('DB_DATABASE')
        self.user = os.getenv('DB_USERNAME')
        self.password = os.getenv('DB_PASSWORD')
        self.ssl = os.getenv('DB_SSL', 'true').lower() == 'true'

        if not all([self.host, self.database, self.user, self.password]):
            raise ValueError("Missing required database environment variables")

        self.connection = None
        self.cursor = None

    def connect(self):
        """Establish database connection."""
        if self.engine == 'postgres':
            self._connect_postgres()
        elif self.engine == 'mysql':
            self._connect_mysql()
        else:
            raise ValueError(f"Unsupported database engine: {self.engine}")

        logger.info(f"Connected to {self.engine} database: {self.database}")

    def _connect_postgres(self):
        """Connect to PostgreSQL/Aurora."""
        import psycopg2

        conn_params = {
            'host': self.host,
            'port': self.port,
            'database': self.database,
            'user': self.user,
            'password': self.password
        }

        if self.ssl:
            conn_params['sslmode'] = 'require'

        self.connection = psycopg2.connect(**conn_params)
        self.cursor = self.connection.cursor()

    def _connect_mysql(self):
        """Connect to MySQL/Aurora."""
        import mysql.connector

        conn_params = {
            'host': self.host,
            'port': self.port,
            'database': self.database,
            'user': self.user,
            'password': self.password
        }

        if self.ssl:
            conn_params['ssl_disabled'] = False

        self.connection = mysql.connector.connect(**conn_params)
        self.cursor = self.connection.cursor()

    def disconnect(self):
        """Close database connection."""
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
        logger.info("Database connection closed")

    def create_table(self, schema: Dict[str, str], has_provider_id: bool = False):
        """
        Create raw_indeed_jobs table based on discovered schema.

        Args:
            schema: Dictionary mapping field names to SQL types
            has_provider_id: Whether provider_id field exists in schema
        """
        if self.engine == 'postgres':
            ddl = self._generate_postgres_ddl(schema, has_provider_id)
        elif self.engine == 'mysql':
            ddl = self._generate_mysql_ddl(schema, has_provider_id)
        else:
            raise ValueError(f"Unsupported engine: {self.engine}")

        logger.info("Creating table raw_indeed_jobs...")
        logger.debug(f"DDL:\n{ddl}")

        try:
            self.cursor.execute(ddl)
            self.connection.commit()
            logger.info("Table raw_indeed_jobs created successfully")
        except Exception as e:
            logger.error(f"Failed to create table: {e}")
            self.connection.rollback()
            raise

    def _generate_postgres_ddl(self, schema: Dict[str, str], has_provider_id: bool) -> str:
        """
        Generate PostgreSQL DDL for raw_indeed_jobs.

        Args:
            schema: Field schema dictionary
            has_provider_id: Include provider_id unique constraint

        Returns:
            DDL string
        """
        columns = [
            "id BIGSERIAL PRIMARY KEY",
            "provider_id TEXT",
            "job_hash TEXT NOT NULL UNIQUE"
        ]

        # Add discovered fields
        for field, sql_type in sorted(schema.items()):
            # Skip if already defined
            if field in ['id', 'provider_id', 'job_hash', 'source_payload', 'ingested_at']:
                continue

            # Map generic types to Postgres-specific
            pg_type = self._map_to_postgres_type(sql_type)
            columns.append(f"{field} {pg_type}")

        # Add required fields
        columns.extend([
            "source_payload JSONB NOT NULL",
            "ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW()"
        ])

        column_defs = ',\n    '.join(columns)
        ddl = f"""
CREATE TABLE IF NOT EXISTS raw_indeed_jobs (
    {column_defs}
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_raw_indeed_jobs_posted_at ON raw_indeed_jobs(posted_at);
CREATE INDEX IF NOT EXISTS idx_raw_indeed_jobs_company ON raw_indeed_jobs(company);
CREATE INDEX IF NOT EXISTS idx_raw_indeed_jobs_ingested_at ON raw_indeed_jobs(ingested_at);
"""

        # Add provider_id unique constraint if stable
        if has_provider_id:
            ddl += "\nCREATE UNIQUE INDEX IF NOT EXISTS idx_raw_indeed_jobs_provider_id ON raw_indeed_jobs(provider_id) WHERE provider_id IS NOT NULL;"

        return ddl

    def _generate_mysql_ddl(self, schema: Dict[str, str], has_provider_id: bool) -> str:
        """
        Generate MySQL DDL for raw_indeed_jobs.

        Args:
            schema: Field schema dictionary
            has_provider_id: Include provider_id unique constraint

        Returns:
            DDL string
        """
        columns = [
            "id BIGINT AUTO_INCREMENT PRIMARY KEY",
            "provider_id VARCHAR(255)",
            "job_hash VARCHAR(64) NOT NULL UNIQUE"
        ]

        # Add discovered fields
        for field, sql_type in sorted(schema.items()):
            if field in ['id', 'provider_id', 'job_hash', 'source_payload', 'ingested_at']:
                continue

            mysql_type = self._map_to_mysql_type(sql_type)
            columns.append(f"{field} {mysql_type}")

        # Add required fields
        columns.extend([
            "source_payload JSON NOT NULL",
            "ingested_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP"
        ])

        # Add indexes
        indexes = [
            "INDEX idx_posted_at (posted_at)",
            "INDEX idx_company (company(100))",
            "INDEX idx_ingested_at (ingested_at)"
        ]

        if has_provider_id:
            indexes.append("UNIQUE INDEX idx_provider_id (provider_id)")

        column_defs = ',\n    '.join(columns)
        index_defs = ',\n    '.join(indexes)
        ddl = f"""
CREATE TABLE IF NOT EXISTS raw_indeed_jobs (
    {column_defs},
    {index_defs}
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
"""

        return ddl

    def _map_to_postgres_type(self, generic_type: str) -> str:
        """Map generic type to PostgreSQL-specific type."""
        mapping = {
            'TEXT': 'TEXT',
            'NUMERIC': 'NUMERIC',
            'BOOLEAN': 'BOOLEAN',
            'TIMESTAMPTZ': 'TIMESTAMPTZ',
            'JSONB': 'JSONB',
            'JSON': 'JSONB'
        }
        return mapping.get(generic_type, 'TEXT')

    def _map_to_mysql_type(self, generic_type: str) -> str:
        """Map generic type to MySQL-specific type."""
        mapping = {
            'TEXT': 'TEXT',
            'NUMERIC': 'DECIMAL(20,2)',
            'BOOLEAN': 'TINYINT(1)',
            'TIMESTAMPTZ': 'DATETIME',
            'JSONB': 'JSON',
            'JSON': 'JSON'
        }
        return mapping.get(generic_type, 'TEXT')

    def insert_jobs(self, jobs: List[Dict[str, Any]]) -> Dict[str, int]:
        """
        Insert job records with deduplication.

        Args:
            jobs: List of normalized job records (with job_hash)

        Returns:
            Dictionary with counts: {inserted, skipped, errors}
        """
        if not jobs:
            return {'inserted': 0, 'skipped': 0, 'errors': 0}

        stats = {'inserted': 0, 'skipped': 0, 'errors': 0}

        for job in jobs:
            try:
                if self.engine == 'postgres':
                    result = self._insert_job_postgres(job)
                elif self.engine == 'mysql':
                    result = self._insert_job_mysql(job)
                else:
                    raise ValueError(f"Unsupported engine: {self.engine}")

                if result:
                    stats['inserted'] += 1
                else:
                    stats['skipped'] += 1

            except Exception as e:
                logger.error(f"Error inserting job: {e}")
                stats['errors'] += 1

        self.connection.commit()
        return stats

    def _insert_job_postgres(self, job: Dict[str, Any]) -> bool:
        """
        Insert job into PostgreSQL with ON CONFLICT handling.

        Args:
            job: Job record with job_hash and source_payload

        Returns:
            True if inserted, False if skipped
        """
        # Get table columns
        self.cursor.execute("""
            SELECT column_name FROM information_schema.columns
            WHERE table_name = 'raw_indeed_jobs'
            AND column_name NOT IN ('id', 'ingested_at')
            ORDER BY ordinal_position
        """)
        columns = [row[0] for row in self.cursor.fetchall()]

        # Build insert values
        values = {}
        for col in columns:
            if col == 'source_payload':
                values[col] = Json(job)
            elif col == 'job_hash':
                values[col] = job.get('job_hash')
            else:
                val = job.get(col)
                # Convert dict/list to Json for JSONB columns
                if isinstance(val, (dict, list)):
                    values[col] = Json(val)
                else:
                    values[col] = val

        # Build SQL
        col_names = ', '.join(columns)
        placeholders = ', '.join(['%s'] * len(columns))
        col_values = [values[col] for col in columns]

        sql = f"""
            INSERT INTO raw_indeed_jobs ({col_names})
            VALUES ({placeholders})
            ON CONFLICT (job_hash) DO NOTHING
            RETURNING id
        """

        self.cursor.execute(sql, col_values)
        result = self.cursor.fetchone()

        return result is not None

    def _insert_job_mysql(self, job: Dict[str, Any]) -> bool:
        """
        Insert job into MySQL with INSERT IGNORE.

        Args:
            job: Job record with job_hash and source_payload

        Returns:
            True if inserted, False if skipped
        """
        # Get table columns
        self.cursor.execute("""
            SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_NAME = 'raw_indeed_jobs'
            AND COLUMN_NAME NOT IN ('id', 'ingested_at')
            ORDER BY ORDINAL_POSITION
        """)
        columns = [row[0] for row in self.cursor.fetchall()]

        # Build insert values
        values = {}
        for col in columns:
            if col == 'source_payload':
                values[col] = json.dumps(job)
            elif col == 'job_hash':
                values[col] = job.get('job_hash')
            else:
                values[col] = job.get(col)

        # Build SQL
        col_names = ', '.join(columns)
        placeholders = ', '.join(['%s'] * len(columns))
        col_values = [values[col] for col in columns]

        sql = f"""
            INSERT IGNORE INTO raw_indeed_jobs ({col_names})
            VALUES ({placeholders})
        """

        self.cursor.execute(sql, col_values)

        # Check if row was inserted
        return self.cursor.rowcount > 0

    def table_exists(self) -> bool:
        """Check if raw_indeed_jobs table exists."""
        if self.engine == 'postgres':
            self.cursor.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables
                    WHERE table_name = 'raw_indeed_jobs'
                )
            """)
        elif self.engine == 'mysql':
            self.cursor.execute("""
                SELECT EXISTS (
                    SELECT * FROM information_schema.tables
                    WHERE table_name = 'raw_indeed_jobs'
                )
            """)

        return self.cursor.fetchone()[0]

    def drop_table(self, table_name: str = 'raw_indeed_jobs'):
        """Drop table if exists."""
        logger.warning(f"Dropping table {table_name}...")
        self.cursor.execute(f"DROP TABLE IF EXISTS {table_name} CASCADE")
        self.connection.commit()
        logger.info(f"Table {table_name} dropped")

    def create_comprehensive_table(self):
        """
        Create comprehensive raw_indeed_jobs table with ALL fields.
        This replaces the old discovery-based schema with a fixed comprehensive schema.
        """
        logger.info("Creating comprehensive raw_indeed_jobs table...")

        if self.engine == 'postgres':
            ddl = self._get_comprehensive_postgres_ddl()
        elif self.engine == 'mysql':
            ddl = self._get_comprehensive_mysql_ddl()
        else:
            raise ValueError(f"Unsupported engine: {self.engine}")

        try:
            # Drop old table if it exists
            self.drop_table()

            # Create new table
            self.cursor.execute(ddl)
            self.connection.commit()
            logger.info("Comprehensive table raw_indeed_jobs created successfully")
        except Exception as e:
            logger.error(f"Failed to create comprehensive table: {e}")
            self.connection.rollback()
            raise

    def _get_comprehensive_postgres_ddl(self) -> str:
        """Generate comprehensive PostgreSQL DDL with all Indeed fields."""
        return """
CREATE TABLE raw_indeed_jobs (
  id BIGSERIAL PRIMARY KEY,

  -- Core identification
  job_key               TEXT,
  provider_id           TEXT,
  job_hash              TEXT UNIQUE NOT NULL,

  -- Basic job info
  title                 TEXT,
  company_name          TEXT,
  company_url           TEXT,
  company_logo_url      TEXT,
  company_header_url    TEXT,

  -- Descriptions (CRITICAL!)
  description_html      TEXT,
  description_text      TEXT,

  -- Job types
  job_types             JSONB,
  job_type_primary      TEXT,

  -- Location (flattened)
  location_city         TEXT,
  location_postal_code  TEXT,
  location_country      TEXT,
  location_country_code TEXT,
  location_fmt_long     TEXT,
  location_fmt_short    TEXT,
  location_latitude     DOUBLE PRECISION,
  location_longitude    DOUBLE PRECISION,
  location_street_address TEXT,
  location_full_address TEXT,

  -- Salary (flattened)
  salary_currency       TEXT,
  salary_max            NUMERIC,
  salary_min            NUMERIC,
  salary_source         TEXT,
  salary_text           TEXT,
  salary_type           TEXT,

  -- Rating
  rating_value          NUMERIC,
  rating_count          INTEGER,

  -- Arrays (as JSONB)
  benefits              JSONB,
  occupations           JSONB,
  attributes            JSONB,
  contacts              JSONB,
  shifts                JSONB,
  social_insurance      JSONB,
  working_system        JSONB,
  shift_and_schedule    JSONB,

  -- Boolean flags
  posted_today          BOOLEAN,
  is_high_volume_hiring BOOLEAN,
  is_urgent_hire        BOOLEAN,
  expired               BOOLEAN,
  is_remote             BOOLEAN,

  -- Dates and metadata
  date_published        TIMESTAMPTZ,
  source_name           TEXT,
  age_text              TEXT,
  locale                TEXT,
  language              TEXT,

  -- URLs
  job_url               TEXT,
  apply_url             TEXT,

  -- Company details
  emails                JSONB,
  company_addresses     JSONB,
  company_num_employees TEXT,
  company_revenue       TEXT,
  company_industry      TEXT,
  company_description   TEXT,
  company_brief_description TEXT,
  company_links         JSONB,
  corporate_website     TEXT,
  company_founded_year  INTEGER,
  company_ceo           JSONB,

  -- Requirements
  requirements          JSONB,

  -- Scraping metadata
  scraping_page         INTEGER,
  scraping_index        INTEGER,
  api_run_id            TEXT,
  meta_name             TEXT,
  meta_note             TEXT,
  meta_max_rows         INTEGER,

  -- Full raw payload
  source_payload        JSONB NOT NULL,
  ingested_at           TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Indexes
CREATE INDEX idx_raw_indeed_jobs_published ON raw_indeed_jobs (date_published);
CREATE INDEX idx_raw_indeed_jobs_company   ON raw_indeed_jobs (company_name);
CREATE INDEX idx_raw_indeed_jobs_ingested  ON raw_indeed_jobs (ingested_at);
CREATE INDEX idx_raw_indeed_jobs_location  ON raw_indeed_jobs (location_city, location_country_code);
CREATE INDEX idx_raw_indeed_jobs_job_key   ON raw_indeed_jobs (job_key) WHERE job_key IS NOT NULL;
"""

    def _get_comprehensive_mysql_ddl(self) -> str:
        """Generate comprehensive MySQL DDL with all Indeed fields."""
        return """
CREATE TABLE raw_indeed_jobs (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,

  -- Core identification
  job_key               VARCHAR(255),
  provider_id           VARCHAR(255),
  job_hash              VARCHAR(64) UNIQUE NOT NULL,

  -- Basic job info
  title                 TEXT,
  company_name          TEXT,
  company_url           TEXT,
  company_logo_url      TEXT,
  company_header_url    TEXT,

  -- Descriptions (CRITICAL!)
  description_html      LONGTEXT,
  description_text      LONGTEXT,

  -- Job types
  job_types             JSON,
  job_type_primary      TEXT,

  -- Location (flattened)
  location_city         TEXT,
  location_postal_code  VARCHAR(20),
  location_country      TEXT,
  location_country_code VARCHAR(10),
  location_fmt_long     TEXT,
  location_fmt_short    TEXT,
  location_latitude     DOUBLE,
  location_longitude    DOUBLE,
  location_street_address TEXT,
  location_full_address TEXT,

  -- Salary (flattened)
  salary_currency       VARCHAR(10),
  salary_max            DECIMAL(20,2),
  salary_min            DECIMAL(20,2),
  salary_source         TEXT,
  salary_text           TEXT,
  salary_type           VARCHAR(50),

  -- Rating
  rating_value          DECIMAL(3,2),
  rating_count          INT,

  -- Arrays (as JSON)
  benefits              JSON,
  occupations           JSON,
  attributes            JSON,
  contacts              JSON,
  shifts                JSON,
  social_insurance      JSON,
  working_system        JSON,
  shift_and_schedule    JSON,

  -- Boolean flags
  posted_today          TINYINT(1),
  is_high_volume_hiring TINYINT(1),
  is_urgent_hire        TINYINT(1),
  expired               TINYINT(1),
  is_remote             TINYINT(1),

  -- Dates and metadata
  date_published        DATETIME,
  source_name           TEXT,
  age_text              TEXT,
  locale                VARCHAR(10),
  language              VARCHAR(10),

  -- URLs
  job_url               TEXT,
  apply_url             TEXT,

  -- Company details
  emails                JSON,
  company_addresses     JSON,
  company_num_employees TEXT,
  company_revenue       TEXT,
  company_industry      TEXT,
  company_description   TEXT,
  company_brief_description TEXT,
  company_links         JSON,
  corporate_website     TEXT,
  company_founded_year  INT,
  company_ceo           JSON,

  -- Requirements
  requirements          JSON,

  -- Scraping metadata
  scraping_page         INT,
  scraping_index        INT,
  api_run_id            VARCHAR(255),
  meta_name             VARCHAR(255),
  meta_note             TEXT,
  meta_max_rows         INT,

  -- Full raw payload
  source_payload        JSON NOT NULL,
  ingested_at           DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,

  INDEX idx_published (date_published),
  INDEX idx_company (company_name(100)),
  INDEX idx_ingested (ingested_at),
  INDEX idx_job_key (job_key)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
"""

    def create_qualified_jobs_table(self):
        """Create qualified_indeed_jobs table for sales-ready jobs."""
        logger.info("Creating qualified_indeed_jobs table...")

        if self.engine == 'postgres':
            ddl = self._get_qualified_jobs_postgres_ddl()
        elif self.engine == 'mysql':
            ddl = self._get_qualified_jobs_mysql_ddl()
        else:
            raise ValueError(f"Unsupported engine: {self.engine}")

        try:
            self.cursor.execute(ddl)
            self.connection.commit()
            logger.info("Table qualified_indeed_jobs created successfully")
        except Exception as e:
            logger.error(f"Failed to create qualified_indeed_jobs table: {e}")
            self.connection.rollback()
            raise

    def _get_qualified_jobs_postgres_ddl(self) -> str:
        """Generate PostgreSQL DDL for qualified_indeed_jobs table."""
        return """
CREATE TABLE IF NOT EXISTS qualified_indeed_jobs (
  id                        BIGSERIAL PRIMARY KEY,
  raw_job_id                BIGINT,
  job_hash                  TEXT UNIQUE,
  job_key                   TEXT,

  title                     TEXT,
  company_name              TEXT,
  location_fmt_short        TEXT,
  date_published            TIMESTAMPTZ,
  salary_text               TEXT,
  job_url                   TEXT,
  apply_url                 TEXT,

  description_text          TEXT,
  description_html          TEXT,

  -- HR contact (best-effort; may be null)
  hr_contact_name           TEXT,
  hr_contact_title          TEXT,
  hr_contact_email          TEXT,
  hr_contact_linkedin       TEXT,

  -- LLM scoring
  score                     INTEGER NOT NULL,
  reasons                   JSONB,
  flags                     JSONB,

  -- Sales prioritization
  company_30d_postings_count INTEGER NOT NULL,

  populated_at              TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_qij_score_desc ON qualified_indeed_jobs (score DESC);
CREATE INDEX IF NOT EXISTS idx_qij_company_count_desc ON qualified_indeed_jobs (company_30d_postings_count DESC);
CREATE INDEX IF NOT EXISTS idx_qij_published ON qualified_indeed_jobs (date_published);
CREATE INDEX IF NOT EXISTS idx_qij_company ON qualified_indeed_jobs (company_name);
"""

    def _get_qualified_jobs_mysql_ddl(self) -> str:
        """Generate MySQL DDL for qualified_indeed_jobs table."""
        return """
CREATE TABLE IF NOT EXISTS qualified_indeed_jobs (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  raw_job_id BIGINT NULL,
  job_hash VARCHAR(128) UNIQUE,
  job_key VARCHAR(255),

  title TEXT,
  company_name TEXT,
  location_fmt_short TEXT,
  date_published DATETIME,
  salary_text TEXT,
  job_url TEXT,
  apply_url TEXT,

  description_text LONGTEXT,
  description_html LONGTEXT,

  hr_contact_name TEXT,
  hr_contact_title TEXT,
  hr_contact_email TEXT,
  hr_contact_linkedin TEXT,

  score INT NOT NULL,
  reasons JSON,
  flags JSON,

  company_30d_postings_count INT NOT NULL,

  populated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,

  INDEX idx_qij_score_desc (score),
  INDEX idx_qij_company_count_desc (company_30d_postings_count),
  INDEX idx_qij_published (date_published),
  INDEX idx_qij_company (company_name(100))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
"""

    def get_company_30d_count(self, company_name: str) -> int:
        """
        Get count of jobs posted by company in last 30 days.

        Args:
            company_name: Company name to count

        Returns:
            Count of jobs
        """
        if not company_name:
            return 0

        try:
            if self.engine == 'postgres':
                sql = """
                    SELECT COUNT(*) AS cnt
                    FROM raw_indeed_jobs
                    WHERE company_name = %s
                      AND date_published >= (NOW() AT TIME ZONE 'UTC') - INTERVAL '30 days'
                """
            elif self.engine == 'mysql':
                sql = """
                    SELECT COUNT(*) AS cnt
                    FROM raw_indeed_jobs
                    WHERE company_name = %s
                      AND date_published >= (UTC_TIMESTAMP() - INTERVAL 30 DAY)
                """
            else:
                return 0

            self.cursor.execute(sql, (company_name,))
            result = self.cursor.fetchone()
            count = result[0] if result else 0

            logger.debug(f"Company {company_name} has {count} jobs in last 30 days")
            return count

        except Exception as e:
            logger.error(f"Error getting company 30d count: {e}")
            return 0

    def upsert_qualified_job(self, job: Dict[str, Any]) -> bool:
        """
        Upsert qualified job into qualified_indeed_jobs table.

        Args:
            job: Qualified job dictionary

        Returns:
            True if inserted/updated, False otherwise
        """
        try:
            if self.engine == 'postgres':
                return self._upsert_qualified_job_postgres(job)
            elif self.engine == 'mysql':
                return self._upsert_qualified_job_mysql(job)
            else:
                raise ValueError(f"Unsupported engine: {self.engine}")
        except Exception as e:
            logger.error(f"Error upserting qualified job: {e}")
            return False

    def _upsert_qualified_job_postgres(self, job: Dict[str, Any]) -> bool:
        """Upsert qualified job into PostgreSQL."""
        # Prepare values
        values = {
            'raw_job_id': job.get('raw_job_id'),
            'job_hash': job.get('job_hash'),
            'job_key': job.get('job_key'),
            'title': job.get('title'),
            'company_name': job.get('company_name'),
            'location_fmt_short': job.get('location_fmt_short'),
            'date_published': job.get('date_published'),
            'salary_text': job.get('salary_text'),
            'job_url': job.get('job_url'),
            'apply_url': job.get('apply_url'),
            'description_text': job.get('description_text'),
            'description_html': job.get('description_html'),
            'hr_contact_name': job.get('hr_contact_name'),
            'hr_contact_title': job.get('hr_contact_title'),
            'hr_contact_email': job.get('hr_contact_email'),
            'hr_contact_linkedin': job.get('hr_contact_linkedin'),
            'score': job.get('score'),
            'reasons': Json(job.get('reasons', [])),
            'flags': Json(job.get('flags', {})),
            'company_30d_postings_count': job.get('company_30d_postings_count', 0)
        }

        sql = """
            INSERT INTO qualified_indeed_jobs (
                raw_job_id, job_hash, job_key, title, company_name, location_fmt_short,
                date_published, salary_text, job_url, apply_url, description_text, description_html,
                hr_contact_name, hr_contact_title, hr_contact_email, hr_contact_linkedin,
                score, reasons, flags, company_30d_postings_count
            )
            VALUES (
                %(raw_job_id)s, %(job_hash)s, %(job_key)s, %(title)s, %(company_name)s, %(location_fmt_short)s,
                %(date_published)s, %(salary_text)s, %(job_url)s, %(apply_url)s, %(description_text)s, %(description_html)s,
                %(hr_contact_name)s, %(hr_contact_title)s, %(hr_contact_email)s, %(hr_contact_linkedin)s,
                %(score)s, %(reasons)s, %(flags)s, %(company_30d_postings_count)s
            )
            ON CONFLICT (job_hash) DO UPDATE SET
                score = EXCLUDED.score,
                reasons = EXCLUDED.reasons,
                flags = EXCLUDED.flags,
                hr_contact_name = EXCLUDED.hr_contact_name,
                hr_contact_title = EXCLUDED.hr_contact_title,
                hr_contact_email = EXCLUDED.hr_contact_email,
                hr_contact_linkedin = EXCLUDED.hr_contact_linkedin,
                company_30d_postings_count = EXCLUDED.company_30d_postings_count,
                populated_at = NOW()
            RETURNING id
        """

        self.cursor.execute(sql, values)
        result = self.cursor.fetchone()
        self.connection.commit()

        return result is not None

    def _upsert_qualified_job_mysql(self, job: Dict[str, Any]) -> bool:
        """Upsert qualified job into MySQL."""
        # Prepare values
        reasons_json = json.dumps(job.get('reasons', []))
        flags_json = json.dumps(job.get('flags', {}))

        sql = """
            INSERT INTO qualified_indeed_jobs (
                raw_job_id, job_hash, job_key, title, company_name, location_fmt_short,
                date_published, salary_text, job_url, apply_url, description_text, description_html,
                hr_contact_name, hr_contact_title, hr_contact_email, hr_contact_linkedin,
                score, reasons, flags, company_30d_postings_count
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                score = VALUES(score),
                reasons = VALUES(reasons),
                flags = VALUES(flags),
                hr_contact_name = VALUES(hr_contact_name),
                hr_contact_title = VALUES(hr_contact_title),
                hr_contact_email = VALUES(hr_contact_email),
                hr_contact_linkedin = VALUES(hr_contact_linkedin),
                company_30d_postings_count = VALUES(company_30d_postings_count),
                populated_at = CURRENT_TIMESTAMP
        """

        values = (
            job.get('raw_job_id'), job.get('job_hash'), job.get('job_key'),
            job.get('title'), job.get('company_name'), job.get('location_fmt_short'),
            job.get('date_published'), job.get('salary_text'), job.get('job_url'),
            job.get('apply_url'), job.get('description_text'), job.get('description_html'),
            job.get('hr_contact_name'), job.get('hr_contact_title'),
            job.get('hr_contact_email'), job.get('hr_contact_linkedin'),
            job.get('score'), reasons_json, flags_json,
            job.get('company_30d_postings_count', 0)
        )

        self.cursor.execute(sql, values)
        self.connection.commit()

        return self.cursor.rowcount > 0
