#!/usr/bin/env python3
"""
CLI wrapper for Supabase sync.
"""
import sys
import os

# Add site-packages to path
site_packages = r'C:\Python311\Lib\site-packages'
if site_packages not in sys.path:
    sys.path.insert(0, site_packages)

# Add backend directory to path
backend_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, backend_dir)

from src.indeed.sync_to_supabase import main

if __name__ == '__main__':
    main()
