#!/usr/bin/env python3
"""Test Supabase connection and check table schema."""
import sys
import os

# Add site-packages to path
site_packages = r'C:\Python311\Lib\site-packages'
if site_packages not in sys.path:
    sys.path.insert(0, site_packages)

from dotenv import load_dotenv
load_dotenv()

try:
    from supabase import create_client
except ImportError:
    print("ERROR: supabase package not installed")
    print("Install with: pip install supabase")
    sys.exit(1)

# Test connection
supabase_url = os.getenv('SUPABASE_URL')
supabase_key = os.getenv('SUPABASE_SERVICE_ROLE_KEY')

print(f"Supabase URL: {supabase_url}")
print(f"Supabase Key: {supabase_key[:20]}..." if supabase_key else "No key")

if not supabase_url or not supabase_key:
    print("\nERROR: SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY must be set in .env")
    sys.exit(1)

try:
    supabase = create_client(supabase_url, supabase_key)
    print("\n✓ Supabase client created successfully")

    # Try to fetch from job_posting_summary table
    print("\nFetching from job_posting_summary table...")
    response = supabase.table('job_posting_summary').select('*').limit(5).execute()

    print(f"\n✓ Successfully fetched {len(response.data)} records")

    if response.data:
        print("\nSample record fields:")
        for key in response.data[0].keys():
            print(f"  - {key}")

        print("\nFirst record:")
        print(response.data[0])
    else:
        print("\nTable is empty")

except Exception as e:
    print(f"\n✗ Error: {e}")
    import traceback
    traceback.print_exc()
