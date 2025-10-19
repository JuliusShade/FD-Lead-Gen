#!/usr/bin/env python3
"""
Create a database view that maps qualified_indeed_jobs to job_posting_summary schema.
"""
import os
from dotenv import load_dotenv
import psycopg2

load_dotenv()

conn = psycopg2.connect(
    host=os.getenv('DB_HOST'),
    port=int(os.getenv('DB_PORT', '5432')),
    database=os.getenv('DB_DATABASE'),
    user=os.getenv('DB_USERNAME'),
    password=os.getenv('DB_PASSWORD'),
    sslmode='require'
)

cur = conn.cursor()

# First, check existing tables
print("\n=== Existing Tables ===")
cur.execute("""
    SELECT tablename
    FROM pg_tables
    WHERE schemaname = 'public'
    ORDER BY tablename
""")
tables = cur.fetchall()
for table in tables:
    print(f"  - {table[0]}")

# Drop existing view if it exists
print("\n=== Dropping existing view (if exists) ===")
cur.execute("DROP VIEW IF EXISTS job_posting_summary CASCADE")
conn.commit()
print("  [OK] Old view dropped")

# Create view that transforms qualified_indeed_jobs to match frontend schema
print("\n=== Creating job_posting_summary view ===")

create_view_sql = """
CREATE VIEW job_posting_summary AS
SELECT
    id,
    job_hash,
    company_name AS company,
    title AS position,

    -- Extract first reason from JSONB array as primary reason
    CASE
        WHEN reasons IS NOT NULL AND jsonb_array_length(reasons) > 0
        THEN reasons->>0
        ELSE NULL
    END AS reason,

    -- Combine all reasons as AI rationale
    CASE
        WHEN reasons IS NOT NULL AND jsonb_array_length(reasons) > 0
        THEN (
            SELECT string_agg(value::text, '; ')
            FROM jsonb_array_elements_text(reasons)
        )
        ELSE NULL
    END AS ai_rationale,

    -- Extract matched keywords from flags as type_of_role array
    CASE
        WHEN flags IS NOT NULL AND flags->'matched_keywords' IS NOT NULL
        THEN (
            SELECT array_agg(value::text)
            FROM jsonb_array_elements_text(flags->'matched_keywords')
        )
        ELSE NULL
    END AS type_of_role,

    -- Company 30-day posting count
    CAST(company_30d_postings_count AS TEXT) AS number_of_positions_last_30,

    -- Estimated opportunity based on salary or score
    CASE
        WHEN salary_text IS NOT NULL THEN salary_text
        WHEN score >= 90 THEN 'High potential (score: ' || score || ')'
        WHEN score >= 85 THEN 'Good fit (score: ' || score || ')'
        ELSE 'Qualified (score: ' || score || ')'
    END AS estimated_opportunity,

    -- Primary contact formatted string
    CASE
        WHEN hr_contact_name IS NOT NULL THEN
            hr_contact_name ||
            COALESCE(' (' || hr_contact_title || ')', '') ||
            COALESCE(' | ' || hr_contact_email, '') ||
            COALESCE(' | ' || hr_contact_linkedin, '')
        ELSE NULL
    END AS primary_contact,

    -- Timestamps
    date_published AS created_at,
    populated_at AS date_processed,

    -- Additional fields for reference
    job_url,
    apply_url,
    location_fmt_short AS location,
    score,
    description_text,
    hr_contact_name,
    hr_contact_title,
    hr_contact_email,
    hr_contact_linkedin

FROM qualified_indeed_jobs
ORDER BY score DESC, populated_at DESC
"""

cur.execute(create_view_sql)
conn.commit()
print("  [OK] View created successfully")

# Test the view
print("\n=== Testing view ===")
cur.execute("""
    SELECT
        id,
        company,
        position,
        score,
        number_of_positions_last_30,
        primary_contact,
        estimated_opportunity
    FROM job_posting_summary
    LIMIT 3
""")

print("\nSample records from view:")
for row in cur.fetchall():
    print(f"\n  ID: {row[0]}")
    print(f"  Company: {row[1]}")
    print(f"  Position: {row[2]}")
    print(f"  Score: {row[3]}")
    print(f"  30d Count: {row[4]}")
    print(f"  Contact: {row[5]}")
    print(f"  Opportunity: {row[6]}")

# Get view column list
print("\n=== View Columns ===")
cur.execute("""
    SELECT column_name, data_type
    FROM information_schema.columns
    WHERE table_name = 'job_posting_summary'
    ORDER BY ordinal_position
""")
for col in cur.fetchall():
    print(f"  - {col[0]}: {col[1]}")

conn.close()
print("\n[OK] Complete!\n")
