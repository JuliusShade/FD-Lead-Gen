"""
Schema discovery for Indeed job data.
Infers database column types from sample API responses.
"""

import logging
from typing import Dict, List, Any, Set
from datetime import datetime

logger = logging.getLogger(__name__)


class SchemaDiscoverer:
    """Discovers schema from sample job records."""

    def __init__(self, sample_size: int = 50):
        """
        Initialize schema discoverer.

        Args:
            sample_size: Number of records to analyze for schema inference
        """
        self.sample_size = sample_size

    def discover_schema(self, jobs: List[Dict[str, Any]]) -> Dict[str, str]:
        """
        Infer schema from job records.

        Args:
            jobs: List of job record dictionaries

        Returns:
            Dictionary mapping field names to SQL types
        """
        if not jobs:
            logger.warning("No jobs provided for schema discovery")
            return {}

        # Limit to sample size
        sample = jobs[:self.sample_size]
        logger.info(f"Analyzing {len(sample)} job records for schema discovery")

        # Collect all unique field names
        all_fields = set()
        for job in sample:
            all_fields.update(self._get_flat_fields(job))

        logger.info(f"Found {len(all_fields)} unique fields")

        # Infer type for each field
        schema = {}
        for field in sorted(all_fields):
            sql_type = self._infer_field_type(field, sample)
            schema[field] = sql_type
            logger.debug(f"  {field}: {sql_type}")

        return schema

    def _get_flat_fields(self, obj: Any, prefix: str = '') -> Set[str]:
        """
        Recursively extract all field paths from nested structure.
        Nested objects become flattened with dots (e.g., "location.city")

        Args:
            obj: Object to flatten
            prefix: Current field prefix

        Returns:
            Set of all field paths
        """
        fields = set()

        if isinstance(obj, dict):
            for key, value in obj.items():
                field_name = f"{prefix}.{key}" if prefix else key

                # For nested objects/arrays, we'll store as JSONB
                if isinstance(value, (dict, list)):
                    fields.add(field_name)
                else:
                    # Add nested fields recursively
                    fields.add(field_name)

        return fields

    def _infer_field_type(self, field: str, sample: List[Dict[str, Any]]) -> str:
        """
        Infer SQL type for a field based on sample values.

        Args:
            field: Field name (may be nested like "location.city")
            sample: List of sample records

        Returns:
            SQL type string (TEXT, NUMERIC, BOOLEAN, TIMESTAMPTZ, JSONB)
        """
        # Collect all non-null values for this field
        values = []
        for record in sample:
            value = self._get_nested_value(record, field)
            if value is not None:
                values.append(value)

        if not values:
            # No values found, default to TEXT
            return 'TEXT'

        # Analyze value types
        types_seen = set()
        for value in values:
            types_seen.add(type(value).__name__)

        # Type inference rules
        # If we see dicts or lists, use JSONB
        if 'dict' in types_seen or 'list' in types_seen:
            return 'JSONB'

        # If all booleans
        if types_seen == {'bool'}:
            return 'BOOLEAN'

        # If we see only numbers
        if types_seen.issubset({'int', 'float'}):
            return 'NUMERIC'

        # Check if string values look like timestamps
        if types_seen == {'str'}:
            if self._looks_like_timestamp(values):
                return 'TIMESTAMPTZ'

        # Default to TEXT
        return 'TEXT'

    def _get_nested_value(self, obj: Dict[str, Any], field: str) -> Any:
        """
        Get value from potentially nested field path.

        Args:
            obj: Dictionary to extract from
            field: Field path (e.g., "location.city")

        Returns:
            Field value or None
        """
        if '.' not in field:
            return obj.get(field)

        parts = field.split('.')
        current = obj
        for part in parts:
            if isinstance(current, dict):
                current = current.get(part)
            else:
                return None
        return current

    def _looks_like_timestamp(self, values: List[str]) -> bool:
        """
        Check if string values look like timestamps.

        Args:
            values: List of string values

        Returns:
            True if majority of values parse as timestamps
        """
        timestamp_count = 0
        total_checked = min(len(values), 10)  # Check first 10 values

        for value in values[:total_checked]:
            if self._is_timestamp(value):
                timestamp_count += 1

        # If >70% look like timestamps, treat as timestamp
        return timestamp_count / total_checked > 0.7

    def _is_timestamp(self, value: str) -> bool:
        """
        Try to parse value as timestamp.

        Args:
            value: String value to check

        Returns:
            True if parseable as timestamp
        """
        if not isinstance(value, str):
            return False

        # Common timestamp formats
        formats = [
            '%Y-%m-%d',
            '%Y-%m-%dT%H:%M:%S',
            '%Y-%m-%dT%H:%M:%S.%f',
            '%Y-%m-%dT%H:%M:%SZ',
            '%Y-%m-%dT%H:%M:%S.%fZ',
            '%Y-%m-%d %H:%M:%S',
            '%m/%d/%Y',
            '%d/%m/%Y',
        ]

        for fmt in formats:
            try:
                datetime.strptime(value, fmt)
                return True
            except (ValueError, TypeError):
                continue

        return False

    def get_core_fields(self) -> Dict[str, str]:
        """
        Get core expected fields for Indeed jobs.
        These are added even if not discovered in sample.

        Returns:
            Dictionary of core field names to SQL types
        """
        return {
            'title': 'TEXT',
            'company': 'TEXT',
            'location': 'TEXT',
            'job_type': 'TEXT',
            'seniority': 'TEXT',
            'remote': 'BOOLEAN',
            'posted_at': 'TIMESTAMPTZ',
            'job_url': 'TEXT',
            'salary_text': 'TEXT',
            'description': 'TEXT'
        }
