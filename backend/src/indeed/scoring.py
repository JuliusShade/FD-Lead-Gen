"""
LLM-based job scoring for packaging/operator role suitability.
Hard fail rule: Jobs requiring U.S. citizenship score 0 and are rejected.
"""

import os
import json
import logging
from typing import Dict, Any, Optional
from openai import OpenAI

logger = logging.getLogger(__name__)


class JobScorer:
    """LLM judge for scoring packaging/operator job suitability."""

    def __init__(self):
        """Initialize OpenAI client with configuration from environment."""
        self.client = OpenAI(api_key=os.getenv('OPENAI_API_KEY'))
        self.model = os.getenv('OPENAI_MODEL', 'gpt-4o-mini')
        self.timeout = int(os.getenv('OPENAI_TIMEOUT_SEC', '30'))
        self.threshold = int(os.getenv('QUALIFY_SCORE_THRESHOLD', '80'))

    def compile_job_json(self, job: Dict[str, Any]) -> Dict[str, Any]:
        """
        Compile compact job JSON for LLM scoring.

        Args:
            job: Job record from raw_indeed_jobs

        Returns:
            Compact job dictionary for scoring
        """
        return {
            'title': job.get('title'),
            'company_name': job.get('company_name'),
            'description_text': job.get('description_text'),
            'description_html': job.get('description_html'),
            'job_types': job.get('job_types'),
            'location_fmt_short': job.get('location_fmt_short'),
            'salary_text': job.get('salary_text'),
            'job_url': job.get('job_url'),
            'apply_url': job.get('apply_url'),
            'date_published': str(job.get('date_published')) if job.get('date_published') else None,
            'attributes': job.get('attributes'),
            'shift_and_schedule': job.get('shift_and_schedule'),
            'is_remote': job.get('is_remote')
        }

    def build_scoring_prompt(self, job_json: Dict[str, Any]) -> str:
        """
        Build LLM scoring prompt from job JSON.

        Args:
            job_json: Compiled job dictionary

        Returns:
            User prompt string
        """
        job_json_str = json.dumps(job_json, indent=2)

        return f"""Evaluate this job for packaging/operator suitability.

Job JSON:
{job_json_str}

Scoring rubric (0–100):
- Role match (0–40): Is this clearly packaging/production/warehouse/machine operator/assembly/forklift/kitting/shipping role?
- Skill/requirements fit (0–25): Minimal specialized credentials; on-the-job training; entry-level acceptable.
- Shift/labor signals (0–15): Mentions shifts, physical labor, standing, lifting, hourly pay, overtime, temp-to-hire.
- Location & on-site (0–10): On-site or plant/warehouse context.
- Language (0–10): Plain, non-professional jargon; minimal degree/licensing.

**Hard FAIL condition:** If the posting explicitly requires "U.S. citizenship" or similar (e.g., "must be a U.S. citizen", "US citizens only"), set `requires_us_citizenship=true` and `score=0` and `recommended=false`.

Output JSON ONLY with this exact shape:
{{
  "score": 0,
  "recommended": false,
  "requires_us_citizenship": false,
  "is_packaging_or_operator_role": false,
  "reasons": ["...","..."],
  "matched_keywords": ["...","..."],
  "red_flags": ["..."],
  "confidence": 0.0
}}

- `recommended` must be true only if score >= {self.threshold} and requires_us_citizenship=false.
- `confidence` in [0.0, 1.0].
- Use `reasons` to justify the score briefly.
- Include `matched_keywords` you used (e.g., "packaging", "warehouse", "operator", "assembly", "forklift", "shipping").
- If you detect any phrasing that demands U.S. citizenship, set `requires_us_citizenship=true` with a clear `red_flags` entry.
"""

    def score_job(self, job: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Score a job using LLM judge.

        Args:
            job: Job record from raw_indeed_jobs

        Returns:
            Scoring result dictionary or None on error
        """
        try:
            # Compile job JSON
            job_json = self.compile_job_json(job)

            # Build prompt
            user_prompt = self.build_scoring_prompt(job_json)

            # System message
            system_message = """You are a recruiting assistant that scores job postings for suitability for low-skill packaging/operator staffing.
You MUST output STRICT JSON only, with no extra text."""

            # Call OpenAI
            logger.debug(f"Scoring job: {job.get('title')} at {job.get('company_name')}")

            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": system_message},
                    {"role": "user", "content": user_prompt}
                ],
                temperature=0.1,
                timeout=self.timeout,
                response_format={"type": "json_object"}
            )

            # Parse response
            content = response.choices[0].message.content.strip()
            result = json.loads(content)

            # Validate structure
            required_fields = ['score', 'recommended', 'requires_us_citizenship',
                             'is_packaging_or_operator_role', 'reasons', 'matched_keywords',
                             'red_flags', 'confidence']

            for field in required_fields:
                if field not in result:
                    logger.warning(f"Missing field in LLM response: {field}")
                    return None

            # Enforce hard fail rule
            if result.get('requires_us_citizenship', False):
                result['score'] = 0
                result['recommended'] = False
                logger.info(f"Hard fail: Job requires U.S. citizenship - {job.get('title')}")

            # Enforce threshold
            if result['score'] < self.threshold:
                result['recommended'] = False

            logger.info(f"Scored: {job.get('title')} - Score: {result['score']}, "
                       f"Recommended: {result['recommended']}, "
                       f"Citizenship required: {result.get('requires_us_citizenship', False)}")

            return result

        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse LLM response as JSON: {e}")
            return None
        except Exception as e:
            logger.error(f"Error scoring job: {e}")
            return None

    def score_job_with_retry(self, job: Dict[str, Any], max_retries: int = 1) -> Optional[Dict[str, Any]]:
        """
        Score a job with retry on JSON parse errors.

        Args:
            job: Job record from raw_indeed_jobs
            max_retries: Maximum number of retries

        Returns:
            Scoring result dictionary or None on error
        """
        result = self.score_job(job)

        if result is None and max_retries > 0:
            logger.info(f"Retrying score for job: {job.get('title')}")
            return self.score_job_with_retry(job, max_retries - 1)

        return result
