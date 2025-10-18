"""
Indeed API client with pagination and polling support.
Handles HTTP requests to RapidAPI Indeed endpoint with exponential backoff.
"""

import os
import time
import logging
import requests
from typing import Dict, List, Optional, Any
from random import uniform

logger = logging.getLogger(__name__)


class IndeedAPIClient:
    """Client for Indeed job scraping via RapidAPI."""

    def __init__(self):
        """Initialize API client with configuration from environment."""
        self.api_key = os.getenv('RAPIDAPI_KEY')
        self.api_host = os.getenv('RAPIDAPI_HOST', 'indeed-scraper-api.p.rapidapi.com')
        self.base_url = os.getenv('INDEED_BASE_URL', 'https://indeed-scraper-api.p.rapidapi.com')
        self.job_path = os.getenv('INDEED_JOB_PATH', '/api/job')
        self.poll_path = os.getenv('INDEED_POLL_PATH', '/api/job/{jobId}')

        # Pagination config
        self.page_param = os.getenv('PAGE_PARAM', 'page')
        self.page_start = int(os.getenv('PAGE_START', '1'))
        self.page_size = int(os.getenv('PAGE_SIZE', '15'))
        self.max_pages = int(os.getenv('MAX_PAGES', '10'))

        if not self.api_key:
            raise ValueError("RAPIDAPI_KEY environment variable is required")

        self.headers = {
            'X-RapidAPI-Key': self.api_key,
            'X-RapidAPI-Host': self.api_host,
            'Content-Type': 'application/json'
        }

    def _make_request(
        self,
        url: str,
        payload: Dict[str, Any],
        max_retries: int = 5
    ) -> Optional[Dict[str, Any]]:
        """
        Make HTTP POST request with exponential backoff on rate limits.

        Args:
            url: Full URL to request
            payload: JSON payload
            max_retries: Maximum number of retry attempts

        Returns:
            Response JSON or None on failure
        """
        for attempt in range(max_retries):
            try:
                response = requests.post(url, json=payload, headers=self.headers, timeout=30)

                # Handle rate limiting
                if response.status_code == 429:
                    if attempt < max_retries - 1:
                        # Exponential backoff with jitter
                        wait_time = (2 ** attempt) + uniform(0, 1)
                        logger.warning(f"Rate limited (429). Retrying in {wait_time:.2f}s...")
                        time.sleep(wait_time)
                        continue
                    else:
                        logger.error("Max retries exceeded for rate limiting")
                        return None

                response.raise_for_status()
                return response.json()

            except requests.exceptions.RequestException as e:
                logger.error(f"Request failed (attempt {attempt + 1}/{max_retries}): {e}")
                if attempt < max_retries - 1:
                    wait_time = (2 ** attempt) + uniform(0, 1)
                    time.sleep(wait_time)
                else:
                    return None

        return None

    def _poll_job_result(
        self,
        job_id: str,
        max_polls: int = 10,
        poll_interval: int = 3
    ) -> Optional[Dict[str, Any]]:
        """
        Poll for job results if API returns jobId first.

        Args:
            job_id: Job ID to poll
            max_polls: Maximum polling attempts
            poll_interval: Seconds between polls

        Returns:
            Job result or None on timeout
        """
        poll_url = f"{self.base_url}{self.poll_path.format(jobId=job_id)}"

        for poll_num in range(max_polls):
            logger.info(f"Polling job {job_id} (attempt {poll_num + 1}/{max_polls})...")

            result = self._make_request(poll_url, {})
            if result and result.get('status') == 'completed':
                logger.info(f"Job {job_id} completed")
                return result

            if poll_num < max_polls - 1:
                time.sleep(poll_interval)

        logger.warning(f"Job {job_id} timed out after {max_polls} polls")
        return None

    def fetch_jobs(
        self,
        query: str,
        location: str,
        from_days: int = 1,
        max_pages: Optional[int] = None,
        job_type: str = "fulltime",
        radius: str = "50",
        sort: str = "relevance",
        country: str = "us"
    ) -> List[Dict[str, Any]]:
        """
        Fetch jobs with pagination.

        Args:
            query: Job search query (e.g., "packaging")
            location: Location string (e.g., "Springfield, OH")
            from_days: Days back to search (1 = last 24h, 30 = last month)
            max_pages: Max pages to fetch (default from env)
            job_type: Type of job (fulltime, parttime, etc.)
            radius: Search radius in miles
            sort: Sort order (relevance, date)
            country: Country code

        Returns:
            List of all job records from all pages
        """
        if max_pages is None:
            max_pages = self.max_pages

        all_jobs = []
        page = self.page_start

        base_payload = {
            "scraper": {
                "maxRows": self.page_size,
                "query": query,
                "location": location,
                "jobType": job_type,
                "radius": radius,
                "sort": sort,
                "fromDays": str(from_days),
                "country": country
            }
        }

        logger.info(f"Starting job fetch: query='{query}', location='{location}', "
                   f"fromDays={from_days}, maxPages={max_pages}")

        for page_num in range(max_pages):
            # Add pagination parameter
            payload = base_payload.copy()
            payload["scraper"][self.page_param] = page + page_num

            logger.info(f"Fetching page {page + page_num} ({page_num + 1}/{max_pages})...")

            url = f"{self.base_url}{self.job_path}"
            result = self._make_request(url, payload)

            if not result:
                logger.warning(f"Page {page + page_num} returned no data, stopping pagination")
                break

            # Handle jobId polling if needed
            if 'jobId' in result and 'data' not in result:
                logger.info(f"Received jobId, polling for results...")
                result = self._poll_job_result(result['jobId'])
                if not result:
                    logger.warning("Polling timed out, stopping pagination")
                    break

            # Extract jobs from response
            jobs = self._extract_jobs_from_response(result)

            if not jobs:
                logger.info(f"Page {page + page_num} returned no jobs, stopping pagination")
                break

            logger.info(f"Page {page + page_num}: received {len(jobs)} jobs")
            all_jobs.extend(jobs)

            # Stop if we got fewer results than expected (last page)
            if len(jobs) < self.page_size:
                logger.info(f"Received fewer than {self.page_size} jobs, assuming last page")
                break

        logger.info(f"Total jobs fetched: {len(all_jobs)} from {page_num + 1} pages")
        return all_jobs

    def _extract_jobs_from_response(self, response: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Extract job records from API response.
        Handles different response structures.

        Args:
            response: API response JSON

        Returns:
            List of job records
        """
        # Log the response structure for debugging
        logger.debug(f"Response keys: {list(response.keys()) if isinstance(response, dict) else 'Not a dict'}")
        logger.debug(f"Response type: {type(response)}")

        # RapidAPI Indeed Scraper structure: returnvalue.data
        if 'returnvalue' in response and isinstance(response['returnvalue'], dict):
            returnvalue = response['returnvalue']
            if 'data' in returnvalue and isinstance(returnvalue['data'], list):
                logger.debug(f"Found {len(returnvalue['data'])} jobs in returnvalue.data")
                return returnvalue['data']

        # Try common response structures
        if 'data' in response:
            data = response['data']
            if isinstance(data, list):
                return data
            elif isinstance(data, dict) and 'jobs' in data:
                return data['jobs']
            elif isinstance(data, dict) and 'results' in data:
                return data['results']

        # Direct jobs array
        if 'jobs' in response:
            return response['jobs']

        # Direct results array
        if 'results' in response:
            return response['results']

        # Response itself might be array
        if isinstance(response, list):
            return response

        # Check if there's a jobId for polling
        if 'jobId' in response:
            logger.debug(f"Response contains jobId: {response.get('jobId')}")
            # This will be handled by the caller
            return []

        logger.warning(f"Unknown response structure: {response}")
        return []

    def test_connection(self) -> bool:
        """
        Test API connection with a minimal request.

        Returns:
            True if connection successful
        """
        test_payload = {
            "scraper": {
                "maxRows": 1,
                "query": "test",
                "location": "New York",
                "fromDays": "1",
                "country": "us"
            }
        }

        url = f"{self.base_url}{self.job_path}"
        result = self._make_request(url, test_payload)

        return result is not None
