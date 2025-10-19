#!/usr/bin/env python3
"""
Wrapper to ensure openai package is importable.
"""
import sys
import os

# Add site-packages to path if needed
site_packages = r'C:\Python311\Lib\site-packages'
if site_packages not in sys.path:
    sys.path.insert(0, site_packages)

# Now run the actual script
if __name__ == '__main__':
    # Change to backend directory
    backend_dir = os.path.dirname(os.path.abspath(__file__))
    os.chdir(backend_dir)

    # Import and run
    sys.path.insert(0, backend_dir)
    from scripts import qualify_indeed_jobs
    qualify_indeed_jobs.main()
