"""
Data normalization and hashing utilities for Indeed job records.
Computes unique job_hash and extracts provider_id.
"""

import hashlib
import logging
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)


def compute_job_hash(job: Dict[str, Any]) -> str:
    """
    Compute SHA-256 hash for job deduplication.

    Hash is based on: provider_id || title || company || location || job_url || posted_at
    Null/missing fields are skipped.

    Args:
        job: Job record dictionary

    Returns:
        64-character hex hash string
    """
    # Fields to include in hash (in order)
    hash_fields = [
        'provider_id',
        'id',
        'title',
        'company',
        'companyName',
        'location',
        'job_url',
        'jobUrl',
        'url',
        'link',
        'posted_at',
        'postedAt',
        'datePosted',
        'posted_date'
    ]

    # Collect non-null values
    hash_parts = []
    for field in hash_fields:
        value = job.get(field)
        if value is not None and str(value).strip():
            hash_parts.append(str(value).strip())

    # If we have no meaningful fields, use entire job JSON as fallback
    if not hash_parts:
        import json
        hash_input = json.dumps(job, sort_keys=True)
    else:
        hash_input = '||'.join(hash_parts)

    # Compute SHA-256
    hash_obj = hashlib.sha256(hash_input.encode('utf-8'))
    job_hash = hash_obj.hexdigest()

    return job_hash


def extract_provider_id(job: Dict[str, Any]) -> Optional[str]:
    """
    Extract provider ID from job record if available.
    Tries common field names.

    Args:
        job: Job record dictionary

    Returns:
        Provider ID string or None
    """
    provider_id_fields = [
        'provider_id',
        'id',
        'jobId',
        'job_id',
        'indeed_id',
        'key'
    ]

    for field in provider_id_fields:
        value = job.get(field)
        if value is not None and str(value).strip():
            return str(value).strip()

    return None


def normalize_job_record(job: Dict[str, Any]) -> Dict[str, Any]:
    """
    Normalize and enrich job record with computed fields.

    Adds:
    - job_hash: Unique hash for deduplication
    - provider_id: Extracted provider ID (if available)

    Args:
        job: Raw job record from API

    Returns:
        Enriched job record with normalized fields
    """
    normalized = job.copy()

    # Compute and add job_hash
    normalized['job_hash'] = compute_job_hash(job)

    # Extract and add provider_id if not already present
    if 'provider_id' not in normalized or not normalized['provider_id']:
        normalized['provider_id'] = extract_provider_id(job)

    return normalized


def flatten_job_fields(job: Dict[str, Any]) -> Dict[str, Any]:
    """
    Flatten common job fields to consistent column names.

    Maps various API field names to standardized schema fields:
    - title
    - company
    - location
    - job_type
    - seniority
    - remote
    - posted_at
    - job_url
    - salary_text
    - description

    Args:
        job: Job record dictionary

    Returns:
        Flattened dictionary with standardized field names
    """
    flattened = {}

    # Title mapping
    title_fields = ['title', 'jobTitle', 'job_title', 'position']
    for field in title_fields:
        if field in job and job[field]:
            flattened['title'] = job[field]
            break

    # Company mapping
    company_fields = ['company', 'companyName', 'company_name', 'employer']
    for field in company_fields:
        if field in job and job[field]:
            flattened['company'] = job[field]
            break

    # Location mapping
    location_fields = ['location', 'jobLocation', 'job_location', 'city']
    for field in location_fields:
        if field in job and job[field]:
            flattened['location'] = job[field]
            break

    # Job type mapping
    job_type_fields = ['jobType', 'job_type', 'employmentType', 'employment_type']
    for field in job_type_fields:
        if field in job and job[field]:
            flattened['job_type'] = job[field]
            break

    # Seniority mapping
    seniority_fields = ['seniority', 'seniorityLevel', 'level', 'experience_level']
    for field in seniority_fields:
        if field in job and job[field]:
            flattened['seniority'] = job[field]
            break

    # Remote mapping
    remote_fields = ['remote', 'isRemote', 'is_remote', 'remoteAllowed']
    for field in remote_fields:
        if field in job:
            flattened['remote'] = job[field]
            break

    # Posted date mapping
    posted_fields = ['posted_at', 'postedAt', 'datePosted', 'posted_date', 'date']
    for field in posted_fields:
        if field in job and job[field]:
            flattened['posted_at'] = job[field]
            break

    # Job URL mapping
    url_fields = ['job_url', 'jobUrl', 'url', 'link', 'applyUrl']
    for field in url_fields:
        if field in job and job[field]:
            flattened['job_url'] = job[field]
            break

    # Salary mapping
    salary_fields = ['salary', 'salary_text', 'salaryText', 'compensation', 'pay']
    for field in salary_fields:
        if field in job and job[field]:
            flattened['salary_text'] = job[field]
            break

    # Description mapping
    desc_fields = ['description', 'jobDescription', 'job_description', 'summary']
    for field in desc_fields:
        if field in job and job[field]:
            flattened['description'] = job[field]
            break

    return flattened
