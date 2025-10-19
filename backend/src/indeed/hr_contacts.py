"""
HR contact sourcing via Apollo.io API.
Finds best HR/recruiting contact for outreach per qualified job.
"""

import os
import json
import logging
import time
from typing import Dict, Any, List, Optional
import requests
from openai import OpenAI

logger = logging.getLogger(__name__)


class HRContactSourcer:
    """Apollo.io-based HR contact finder with LLM tie-breaker."""

    # Priority titles (iterate until results)
    PRIORITY_TITLES = [
        "VP of Human Resources",
        "Head of Human Resources",
        "Director of Human Resources",
        "HR Business Partner",
        "HR Manager",
        "HR Generalist",
        "People Operations",
        "Talent Acquisition",
        "Recruiting Manager",
        "Recruiter",
        "Plant HR Manager",
        "Staffing Specialist"
    ]

    def __init__(self):
        """Initialize Apollo.io client and OpenAI for tie-breaking."""
        self.api_key = os.getenv('APOLLO_IO_API_KEY')
        self.org_search_url = os.getenv('APOLLO_ORG_SEARCH_URL',
                                       'https://api.apollo.io/api/v1/mixed_companies/search')
        self.people_search_url = os.getenv('APOLLO_PEOPLE_SEARCH_URL',
                                          'https://api.apollo.io/api/v1/mixed_people/search')
        self.per_page = int(os.getenv('HR_SEARCH_PER_PAGE', '10'))
        self.max_titles = int(os.getenv('HR_SEARCH_MAX_TITLES', '12'))
        self.rate_limit_sleep = float(os.getenv('RATE_LIMIT_SLEEP_SEC', '0.5'))

        # OpenAI for tie-breaking
        self.openai_client = OpenAI(api_key=os.getenv('OPENAI_API_KEY'))
        self.openai_model = os.getenv('OPENAI_MODEL', 'gpt-4o-mini')

    def search_organization(self, company_name: str) -> Optional[Dict[str, Any]]:
        """
        Search Apollo.io for organization by company name.

        Args:
            company_name: Company name to search

        Returns:
            Organization data or None if not found
        """
        if not self.api_key:
            logger.warning("APOLLO_IO_API_KEY not set, skipping org search")
            return None

        try:
            headers = {
                'Content-Type': 'application/json',
                'Cache-Control': 'no-cache',
                'X-Api-Key': self.api_key
            }

            payload = {
                'q_organization_name': company_name,
                'page': 1,
                'per_page': 1
            }

            logger.debug(f"Searching Apollo for organization: {company_name}")

            response = requests.post(
                self.org_search_url,
                headers=headers,
                json=payload,
                timeout=30
            )

            time.sleep(self.rate_limit_sleep)

            if response.status_code != 200:
                logger.warning(f"Apollo org search failed: {response.status_code} - {response.text}")
                return None

            data = response.json()
            organizations = data.get('organizations', [])

            if not organizations:
                logger.info(f"No organization found for: {company_name}")
                return None

            org = organizations[0]
            logger.info(f"Found organization: {org.get('name')} (ID: {org.get('id')})")
            return org

        except Exception as e:
            logger.error(f"Error searching organization: {e}")
            return None

    def search_hr_contacts(self, organization_id: str, title: str) -> List[Dict[str, Any]]:
        """
        Search Apollo.io for HR contacts at organization.

        Args:
            organization_id: Apollo organization ID
            title: Job title to search for

        Returns:
            List of contact dictionaries
        """
        if not self.api_key:
            return []

        try:
            headers = {
                'Content-Type': 'application/json',
                'Cache-Control': 'no-cache',
                'X-Api-Key': self.api_key
            }

            payload = {
                'organization_ids': [organization_id],
                'person_titles': [title],
                'page': 1,
                'per_page': self.per_page
            }

            logger.debug(f"Searching for title: {title} at org {organization_id}")

            response = requests.post(
                self.people_search_url,
                headers=headers,
                json=payload,
                timeout=30
            )

            time.sleep(self.rate_limit_sleep)

            if response.status_code != 200:
                logger.warning(f"Apollo people search failed: {response.status_code}")
                return []

            data = response.json()
            people = data.get('people', [])

            logger.info(f"Found {len(people)} contacts for title: {title}")
            return people

        except Exception as e:
            logger.error(f"Error searching HR contacts: {e}")
            return []

    def aggregate_contacts(self, organization_id: str) -> List[Dict[str, Any]]:
        """
        Aggregate HR contacts across priority titles.

        Args:
            organization_id: Apollo organization ID

        Returns:
            List of contact dictionaries with name, title, email, linkedin
        """
        all_contacts = []
        titles_checked = 0

        for title in self.PRIORITY_TITLES[:self.max_titles]:
            contacts = self.search_hr_contacts(organization_id, title)

            for person in contacts:
                contact = {
                    'name': person.get('name'),
                    'title': person.get('title'),
                    'email': person.get('email'),
                    'linkedin': person.get('linkedin_url'),
                    'priority_level': titles_checked  # Lower is higher priority
                }
                all_contacts.append(contact)

            titles_checked += 1

            # If we found contacts, we can stop early (or continue for more options)
            if contacts:
                break

        logger.info(f"Aggregated {len(all_contacts)} total contacts")
        return all_contacts

    def select_best_contact_rule_based(self, contacts: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        """
        Select best HR contact using rule-based logic.

        Args:
            contacts: List of contact dictionaries

        Returns:
            Best contact or None
        """
        if not contacts:
            return None

        # Filter to contacts with email or LinkedIn
        valid_contacts = [c for c in contacts if c.get('email') or c.get('linkedin')]

        if not valid_contacts:
            # Fallback to any contact
            valid_contacts = contacts

        # Sort by priority level (lower is better), then prefer contacts with email
        def contact_score(c):
            score = c.get('priority_level', 999)
            if c.get('email'):
                score -= 0.5  # Slight preference for email
            return score

        sorted_contacts = sorted(valid_contacts, key=contact_score)
        return sorted_contacts[0] if sorted_contacts else None

    def select_best_contact_llm(self, contacts: List[Dict[str, Any]],
                                company_name: str, job_title: str,
                                location: str) -> Optional[Dict[str, Any]]:
        """
        Use LLM to select best HR contact from list.

        Args:
            contacts: List of contact dictionaries
            company_name: Company name
            job_title: Job title
            location: Job location

        Returns:
            Selected contact or None
        """
        if not contacts:
            return None

        try:
            # Prepare contacts for LLM (top 5 for token efficiency)
            top_contacts = contacts[:5]
            contacts_json = json.dumps(top_contacts, indent=2)

            system_message = """You are a sales outreach assistant. From a list of HR contacts, pick ONE best contact for staffing outreach.
Return STRICT JSON with fields: { "name": "", "title": "", "email": "", "linkedin": "", "reason": "" }. No extra text."""

            user_message = f"""Company: {company_name}
Role: {job_title}
Location: {location}

Contacts:
{contacts_json}

Selection rules:
- Prefer senior HR leadership (VP/Director/Head) > HRBP/Manager > Recruiter.
- Prefer contacts likely tied to the hiring site/region if present.
- If multiple equal, pick the one with an email or LinkedIn.
Return JSON only."""

            response = self.openai_client.chat.completions.create(
                model=self.openai_model,
                messages=[
                    {"role": "system", "content": system_message},
                    {"role": "user", "content": user_message}
                ],
                temperature=0.1,
                timeout=30,
                response_format={"type": "json_object"}
            )

            content = response.choices[0].message.content.strip()
            selected = json.loads(content)

            logger.info(f"LLM selected contact: {selected.get('name')} ({selected.get('title')})")
            return selected

        except Exception as e:
            logger.error(f"Error in LLM contact selection: {e}")
            return None

    def find_best_hr_contact(self, company_name: str, job_title: str = "",
                            location: str = "") -> Optional[Dict[str, Any]]:
        """
        Find best HR contact for a company.

        Args:
            company_name: Company name
            job_title: Job title (for context)
            location: Job location (for context)

        Returns:
            Best HR contact dict with name, title, email, linkedin or None
        """
        if not company_name:
            logger.warning("No company name provided")
            return None

        # 1. Search for organization
        org = self.search_organization(company_name)
        if not org:
            logger.info(f"No organization found for: {company_name}")
            return None

        org_id = org.get('id')
        if not org_id:
            logger.warning("Organization has no ID")
            return None

        # 2. Aggregate HR contacts
        contacts = self.aggregate_contacts(org_id)
        if not contacts:
            logger.info(f"No HR contacts found for: {company_name}")
            return None

        # 3. Select best contact
        # Try LLM first, fallback to rule-based
        best_contact = self.select_best_contact_llm(contacts, company_name, job_title, location)

        if not best_contact:
            logger.info("LLM selection failed, using rule-based selection")
            best_contact = self.select_best_contact_rule_based(contacts)

        if best_contact:
            logger.info(f"Selected HR contact: {best_contact.get('name')} - {best_contact.get('title')}")

        return best_contact
