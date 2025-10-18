#!/usr/bin/env python3
"""
Validation script to check data capture quality.
Validates that descriptions and other critical fields are populated.
"""

import os
import sys
from pathlib import Path
from dotenv import load_dotenv
import psycopg2

# Load environment
backend_dir = Path(__file__).parent.parent
load_dotenv(backend_dir / '.env')


def validate_data():
    """Validate data capture in raw_indeed_jobs table."""

    # Connect to database
    conn = psycopg2.connect(
        host=os.getenv('DB_HOST'),
        port=int(os.getenv('DB_PORT', '5432')),
        database=os.getenv('DB_DATABASE'),
        user=os.getenv('DB_USERNAME'),
        password=os.getenv('DB_PASSWORD'),
        sslmode='require' if os.getenv('DB_SSL', 'true').lower() == 'true' else 'disable'
    )

    cursor = conn.cursor()

    print("\n=== DATA VALIDATION REPORT ===\n")

    # Total count
    cursor.execute("SELECT COUNT(*) FROM raw_indeed_jobs")
    total_count = cursor.fetchone()[0]
    print(f"Total jobs in database: {total_count}")

    if total_count == 0:
        print("\nNo data to validate!")
        return

    # Description HTML populated
    cursor.execute("""
        SELECT COUNT(*)
        FROM raw_indeed_jobs
        WHERE description_html IS NOT NULL
        AND description_html != ''
    """)
    desc_html_count = cursor.fetchone()[0]
    desc_html_pct = (desc_html_count / total_count) * 100

    # Description text populated
    cursor.execute("""
        SELECT COUNT(*)
        FROM raw_indeed_jobs
        WHERE description_text IS NOT NULL
        AND description_text != ''
    """)
    desc_text_count = cursor.fetchone()[0]
    desc_text_pct = (desc_text_count / total_count) * 100

    # Either description populated
    cursor.execute("""
        SELECT COUNT(*)
        FROM raw_indeed_jobs
        WHERE (description_html IS NOT NULL AND description_html != '')
        OR (description_text IS NOT NULL AND description_text != '')
    """)
    any_desc_count = cursor.fetchone()[0]
    any_desc_pct = (any_desc_count / total_count) * 100

    print(f"\n>> Description Fields:")
    print(f"  description_html populated: {desc_html_count}/{total_count} ({desc_html_pct:.1f}%)")
    print(f"  description_text populated: {desc_text_count}/{total_count} ({desc_text_pct:.1f}%)")
    print(f"  Either description populated: {any_desc_count}/{total_count} ({any_desc_pct:.1f}%)")

    if any_desc_pct >= 80:
        print(f"  [PASS] {any_desc_pct:.1f}% >= 80% threshold")
    else:
        print(f"  [FAIL] {any_desc_pct:.1f}% < 80% threshold")

    # Check other critical fields
    critical_fields = [
        ('title', 'Job Title'),
        ('company_name', 'Company Name'),
        ('location_city', 'Location City'),
        ('job_url', 'Job URL'),
        ('date_published', 'Date Published')
    ]

    print(f"\n>> Critical Fields:")
    for field, label in critical_fields:
        cursor.execute(f"""
            SELECT COUNT(*)
            FROM raw_indeed_jobs
            WHERE {field} IS NOT NULL
            AND {field}::text != ''
        """)
        count = cursor.fetchone()[0]
        pct = (count / total_count) * 100
        status = "[OK]" if pct >= 90 else "[WARN]"
        print(f"  {status} {label}: {count}/{total_count} ({pct:.1f}%)")

    # Check JSON/array fields
    json_fields = [
        ('job_types', 'Job Types'),
        ('benefits', 'Benefits'),
        ('occupations', 'Occupations'),
        ('attributes', 'Attributes')
    ]

    print(f"\n>> JSON/Array Fields:")
    for field, label in json_fields:
        cursor.execute(f"""
            SELECT COUNT(*)
            FROM raw_indeed_jobs
            WHERE {field} IS NOT NULL
            AND jsonb_array_length({field}) > 0
        """)
        count = cursor.fetchone()[0]
        pct = (count / total_count) * 100
        print(f"  {label}: {count}/{total_count} ({pct:.1f}%)")

    # Sample record details
    print(f"\n>> Sample Record (most recent):")
    cursor.execute("""
        SELECT
            title,
            company_name,
            location_city,
            LENGTH(description_html) as desc_html_len,
            LENGTH(description_text) as desc_text_len,
            job_types,
            salary_text,
            date_published,
            ingested_at
        FROM raw_indeed_jobs
        ORDER BY ingested_at DESC
        LIMIT 1
    """)

    row = cursor.fetchone()
    if row:
        print(f"  Title: {row[0]}")
        print(f"  Company: {row[1]}")
        print(f"  Location: {row[2]}")
        print(f"  Description HTML length: {row[3]} chars")
        print(f"  Description Text length: {row[4]} chars")
        print(f"  Job Types: {row[5]}")
        print(f"  Salary: {row[6]}")
        print(f"  Published: {row[7]}")
        print(f"  Ingested: {row[8]}")

    cursor.close()
    conn.close()

    print("\n=== VALIDATION COMPLETE ===\n")


if __name__ == '__main__':
    try:
        validate_data()
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)
