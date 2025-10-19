#!/usr/bin/env python3
"""Check qualified jobs results."""
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

print("\n" + "=" * 100)
print("QUALIFIED JOBS SUMMARY")
print("=" * 100)

# Total count
cur.execute("SELECT COUNT(*) FROM qualified_indeed_jobs")
total = cur.fetchone()[0]
print(f"\nTotal qualified jobs: {total}")

# Score distribution
cur.execute("""
    SELECT
        AVG(score) as avg_score,
        MIN(score) as min_score,
        MAX(score) as max_score
    FROM qualified_indeed_jobs
""")
row = cur.fetchone()
print(f"Average score: {row[0]:.1f}")
print(f"Score range: {row[1]} - {row[2]}")

# HR contact stats
cur.execute("""
    SELECT COUNT(*)
    FROM qualified_indeed_jobs
    WHERE hr_contact_name IS NOT NULL
""")
with_hr = cur.fetchone()[0]
hr_pct = (with_hr / total * 100) if total > 0 else 0
print(f"\nJobs with HR contacts: {with_hr}/{total} ({hr_pct:.1f}%)")

# Company distribution
print("\n" + "-" * 100)
print("TOP COMPANIES BY QUALIFIED JOBS")
print("-" * 100)
cur.execute("""
    SELECT
        company_name,
        COUNT(*) as job_count,
        AVG(score) as avg_score,
        MAX(company_30d_postings_count) as company_30d_count,
        COUNT(hr_contact_name) as hr_contacts_found
    FROM qualified_indeed_jobs
    GROUP BY company_name
    ORDER BY job_count DESC, avg_score DESC
    LIMIT 10
""")

for row in cur.fetchall():
    print(f"\n{row[0]}")
    print(f"  Qualified jobs: {row[1]}")
    print(f"  Avg score: {row[2]:.1f}")
    print(f"  30-day postings: {row[3]}")
    print(f"  HR contacts: {row[4]}")

# Sample qualified jobs
print("\n" + "-" * 100)
print("SAMPLE QUALIFIED JOBS (Top 5 by score)")
print("-" * 100)
cur.execute("""
    SELECT
        title,
        company_name,
        score,
        hr_contact_name,
        hr_contact_title,
        company_30d_postings_count,
        populated_at
    FROM qualified_indeed_jobs
    ORDER BY score DESC, populated_at DESC
    LIMIT 5
""")

for row in cur.fetchall():
    print(f"\nTitle: {row[0]}")
    print(f"Company: {row[1]}")
    print(f"Score: {row[2]}")
    print(f"HR Contact: {row[3]} ({row[4]})" if row[3] else "HR Contact: None")
    print(f"Company 30d postings: {row[5]}")
    print(f"Qualified at: {row[6]}")

conn.close()
print("\n" + "=" * 100 + "\n")
