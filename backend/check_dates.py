#!/usr/bin/env python3
"""Quick script to check ingestion dates."""
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

print("\nMost recent ingested jobs:")
print("=" * 100)

cur.execute("""
    SELECT title, company_name, ingested_at, date_published
    FROM raw_indeed_jobs
    ORDER BY ingested_at DESC
    LIMIT 5
""")

for row in cur.fetchall():
    print(f"Title: {row[0]}")
    print(f"Company: {row[1]}")
    print(f"Ingested At: {row[2]}")
    print(f"Date Published: {row[3]}")
    print("-" * 100)

cur.execute("SELECT NOW() AT TIME ZONE 'UTC' as current_utc")
current_time = cur.fetchone()[0]
print(f"\nCurrent UTC time: {current_time}")

cur.execute("""
    SELECT COUNT(*)
    FROM raw_indeed_jobs
    WHERE ingested_at >= (NOW() AT TIME ZONE 'UTC') - INTERVAL '24 hours'
""")
count_24h = cur.fetchone()[0]
print(f"Jobs ingested in last 24 hours: {count_24h}")

cur.execute("""
    SELECT COUNT(*)
    FROM raw_indeed_jobs
    WHERE ingested_at >= (NOW() AT TIME ZONE 'UTC') - INTERVAL '3 hours'
""")
count_3h = cur.fetchone()[0]
print(f"Jobs ingested in last 3 hours: {count_3h}")

conn.close()
