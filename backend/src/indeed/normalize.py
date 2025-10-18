"""
Data normalization and field extraction for Indeed job records.
Extracts ALL fields from the API response including nested structures.
"""

import hashlib
import logging
import json
from typing import Dict, Any, Optional
from datetime import datetime

logger = logging.getLogger(__name__)

# Import BeautifulSoup for HTML stripping
try:
    from bs4 import BeautifulSoup
    HAS_BS4 = True
except ImportError:
    HAS_BS4 = False
    logger.warning("BeautifulSoup not available - HTML stripping disabled")


def strip_html(html: str) -> str:
    """
    Strip HTML tags and return plain text.

    Args:
        html: HTML string

    Returns:
        Plain text with whitespace collapsed
    """
    if not html or not HAS_BS4:
        return html or ""

    try:
        soup = BeautifulSoup(html, 'html.parser')
        text = soup.get_text(" ", strip=True)
        # Collapse multiple spaces/newlines
        text = ' '.join(text.split())
        return text
    except Exception as e:
        logger.warning(f"Failed to strip HTML: {e}")
        return html


def safe_get(obj: Any, *keys, default=None):
    """
    Safely navigate nested dict/list structures.

    Args:
        obj: Object to navigate
        *keys: Path to navigate (supports dict keys and list indices)
        default: Default value if path not found

    Returns:
        Value at path or default
    """
    current = obj
    for key in keys:
        if current is None:
            return default
        if isinstance(current, dict):
            current = current.get(key)
        elif isinstance(current, list) and isinstance(key, int):
            try:
                current = current[key]
            except (IndexError, TypeError):
                return default
        else:
            return default
    return current if current is not None else default


def compute_job_hash(job: Dict[str, Any]) -> str:
    """
    Compute SHA-256 hash for job deduplication.

    Hash components: job_key | title | company_name | job_url | date_published

    Args:
        job: Normalized job record

    Returns:
        64-character hex hash
    """
    parts = [
        str(job.get('job_key') or ''),
        str(job.get('title') or ''),
        str(job.get('company_name') or ''),
        str(job.get('job_url') or ''),
        str(job.get('date_published') or '')
    ]

    hash_input = '|'.join(parts)
    return hashlib.sha256(hash_input.encode('utf-8')).hexdigest()


def normalize_job_record(raw_job: Dict[str, Any]) -> Dict[str, Any]:
    """
    Extract and normalize ALL fields from Indeed API job record.

    Args:
        raw_job: Raw job object from API response

    Returns:
        Normalized dictionary with all fields mapped to database columns
    """
    normalized = {}

    # === Core identification ===
    normalized['job_key'] = safe_get(raw_job, 'jobKey')
    normalized['provider_id'] = safe_get(raw_job, 'id') or safe_get(raw_job, 'jobId')

    # === Basic job info ===
    normalized['title'] = safe_get(raw_job, 'title')
    normalized['company_name'] = safe_get(raw_job, 'companyName')
    normalized['company_url'] = safe_get(raw_job, 'companyUrl')
    normalized['company_logo_url'] = safe_get(raw_job, 'companyLogoUrl')
    normalized['company_header_url'] = safe_get(raw_job, 'companyHeaderUrl')

    # === Descriptions (CRITICAL - was missing!) ===
    description_html = safe_get(raw_job, 'descriptionHtml')
    description_text = safe_get(raw_job, 'descriptionText')

    normalized['description_html'] = description_html
    normalized['description_text'] = description_text

    # If we have HTML but no text, derive it
    if description_html and not description_text:
        normalized['description_text'] = strip_html(description_html)

    # === Job types (array) ===
    job_types = safe_get(raw_job, 'jobType', default=[])
    normalized['job_types'] = job_types if isinstance(job_types, list) else [job_types] if job_types else []
    normalized['job_type_primary'] = job_types[0] if job_types and isinstance(job_types, list) else job_types

    # === Location (nested object) ===
    location = safe_get(raw_job, 'location', default={})
    normalized['location_city'] = safe_get(location, 'city')
    normalized['location_postal_code'] = safe_get(location, 'postalCode')
    normalized['location_country'] = safe_get(location, 'country')
    normalized['location_country_code'] = safe_get(location, 'countryCode')
    normalized['location_fmt_long'] = safe_get(location, 'formattedAddressLong')
    normalized['location_fmt_short'] = safe_get(location, 'formattedAddressShort')
    normalized['location_latitude'] = safe_get(location, 'latitude')
    normalized['location_longitude'] = safe_get(location, 'longitude')
    normalized['location_street_address'] = safe_get(location, 'streetAddress')
    normalized['location_full_address'] = safe_get(location, 'fullAddress')

    # === Salary (nested object) ===
    salary = safe_get(raw_job, 'salary', default={})
    normalized['salary_currency'] = safe_get(salary, 'salaryCurrency')
    normalized['salary_max'] = safe_get(salary, 'salaryMax')
    normalized['salary_min'] = safe_get(salary, 'salaryMin')
    normalized['salary_source'] = safe_get(salary, 'salarySource')
    normalized['salary_text'] = safe_get(salary, 'salaryText')
    normalized['salary_type'] = safe_get(salary, 'salaryType')

    # === Rating ===
    rating = safe_get(raw_job, 'rating', default={})
    normalized['rating_value'] = safe_get(rating, 'rating')
    normalized['rating_count'] = safe_get(rating, 'count')

    # === Arrays (keep as JSON) ===
    normalized['benefits'] = safe_get(raw_job, 'benefits', default=[])
    normalized['occupations'] = safe_get(raw_job, 'occupation', default=[])
    normalized['attributes'] = safe_get(raw_job, 'attributes', default=[])
    normalized['contacts'] = safe_get(raw_job, 'contacts', default=[])
    normalized['shifts'] = safe_get(raw_job, 'shifts', default=[])
    normalized['social_insurance'] = safe_get(raw_job, 'socialInsurance', default=[])
    normalized['working_system'] = safe_get(raw_job, 'workingSystem', default=[])
    normalized['shift_and_schedule'] = safe_get(raw_job, 'shiftAndSchedule', default=[])

    # === Boolean flags ===
    normalized['posted_today'] = safe_get(raw_job, 'postedToday', default=False)

    hiring_demand = safe_get(raw_job, 'hiringDemand', default={})
    normalized['is_high_volume_hiring'] = safe_get(hiring_demand, 'isHighVolumeHiring', default=False)
    normalized['is_urgent_hire'] = safe_get(hiring_demand, 'isUrgentHire', default=False)

    normalized['expired'] = safe_get(raw_job, 'expired', default=False)
    normalized['is_remote'] = safe_get(raw_job, 'isRemote', default=False)

    # === Dates and metadata ===
    normalized['date_published'] = safe_get(raw_job, 'datePublished')
    normalized['source_name'] = safe_get(raw_job, 'source')
    normalized['age_text'] = safe_get(raw_job, 'age')
    normalized['locale'] = safe_get(raw_job, 'locale')
    normalized['language'] = safe_get(raw_job, 'language')

    # === URLs ===
    normalized['job_url'] = safe_get(raw_job, 'jobUrl')
    normalized['apply_url'] = safe_get(raw_job, 'applyUrl')

    # === Company details (arrays/objects) ===
    normalized['emails'] = safe_get(raw_job, 'emails', default=[])
    normalized['company_addresses'] = safe_get(raw_job, 'companyAddresses', default=[])
    normalized['company_num_employees'] = safe_get(raw_job, 'companyNumEmployees')
    normalized['company_revenue'] = safe_get(raw_job, 'companyRevenue')
    normalized['company_industry'] = safe_get(raw_job, 'companyIndustry')
    normalized['company_description'] = safe_get(raw_job, 'companyDescription')
    normalized['company_brief_description'] = safe_get(raw_job, 'companyBriefDescription')

    # Company links
    company_links = safe_get(raw_job, 'companyLinks', default={})
    normalized['company_links'] = company_links
    normalized['corporate_website'] = safe_get(company_links, 'corporateWebsite')

    # Company founded
    company_founded = safe_get(raw_job, 'companyFounded', default={})
    normalized['company_founded_year'] = safe_get(company_founded, 'year')

    # CEO
    normalized['company_ceo'] = safe_get(raw_job, 'companyCeo', default={})

    # === Requirements ===
    normalized['requirements'] = safe_get(raw_job, 'requirements', default=[])

    # === Scraping metadata ===
    scraping_info = safe_get(raw_job, 'scrapingInfo', default={})
    normalized['scraping_page'] = safe_get(scraping_info, 'page')
    normalized['scraping_index'] = safe_get(scraping_info, 'index')

    # === Compute hash ===
    normalized['job_hash'] = compute_job_hash(normalized)

    return normalized


def extract_meta_fields(response: Dict[str, Any]) -> Dict[str, Any]:
    """
    Extract metadata fields from API response.

    Args:
        response: Full API response

    Returns:
        Dictionary with metadata fields
    """
    meta = {}

    # Top-level meta
    meta['meta_name'] = safe_get(response, 'name')
    meta['meta_note'] = safe_get(safe_get(response, 'meta', default={}), 'note')
    meta['meta_max_rows'] = safe_get(safe_get(response, 'meta', default={}), 'max_rows_per_request')

    # Data/scraper run ID
    meta['api_run_id'] = safe_get(response, 'id')

    # Scraper request info
    scraper_data = safe_get(response, 'data', 'scraper', default={})
    if scraper_data:
        meta['api_run_id'] = meta['api_run_id'] or safe_get(scraper_data, 'id')

    return meta
