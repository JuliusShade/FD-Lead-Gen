#!/usr/bin/env python3
"""
Simple Flask API server to serve qualified jobs data to the frontend.
"""
import os
import sys
from flask import Flask, jsonify, request
from flask_cors import CORS
from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import RealDictCursor
import logging

# Add site-packages to path
site_packages = r'C:\Python311\Lib\site-packages'
if site_packages not in sys.path:
    sys.path.insert(0, site_packages)

load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s'
)
logger = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app)  # Enable CORS for frontend requests


def get_db_connection():
    """Create database connection."""
    return psycopg2.connect(
        host=os.getenv('DB_HOST'),
        port=int(os.getenv('DB_PORT', '5432')),
        database=os.getenv('DB_DATABASE'),
        user=os.getenv('DB_USERNAME'),
        password=os.getenv('DB_PASSWORD'),
        sslmode='require' if os.getenv('DB_SSL', 'true').lower() == 'true' else 'prefer'
    )


@app.route('/api/health', methods=['GET'])
def health_check():
    """Health check endpoint."""
    return jsonify({'status': 'ok', 'service': 'FD Lead Gen API'})


@app.route('/api/jobs/summary', methods=['GET'])
def get_job_posting_summary():
    """
    Get job posting summary data from the database view.

    Query parameters:
    - limit: Max number of records (default: 100)
    - offset: Pagination offset (default: 0)
    - min_score: Minimum score filter (default: 0)
    """
    limit = request.args.get('limit', 100, type=int)
    offset = request.args.get('offset', 0, type=int)
    min_score = request.args.get('min_score', 0, type=int)

    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)

        # Query qualified_indeed_jobs table directly with all sales-relevant fields
        query = """
            SELECT
                id,
                company_name,
                title,
                location_fmt_short,
                score,
                salary_text,
                job_url,
                reasons,
                flags,
                company_30d_postings_count,
                hr_contact_name,
                hr_contact_title,
                hr_contact_email,
                hr_contact_linkedin,
                date_published,
                populated_at
            FROM qualified_indeed_jobs
            WHERE score >= %s
            ORDER BY score DESC, populated_at DESC
            LIMIT %s OFFSET %s
        """

        cursor.execute(query, (min_score, limit, offset))
        jobs = cursor.fetchall()

        # Convert to list of dicts (RealDictCursor gives dict-like rows)
        result = []
        for job in jobs:
            # Extract reasons from JSONB
            reasons = job.get('reasons') or []
            reason_text = reasons[0] if reasons else None
            ai_rationale_text = "; ".join(reasons) if reasons else None

            # Extract type_of_role from flags
            flags = job.get('flags') or {}
            matched_keywords = flags.get('matched_keywords', [])

            result.append({
                'id': job['id'],
                'company': job['company_name'],
                'position': job['title'],
                'location': job['location_fmt_short'],
                'score': job['score'],
                'salary_text': job['salary_text'],
                'job_url': job['job_url'],
                'reason': reason_text,
                'ai_rationale': ai_rationale_text,
                'type_of_role': matched_keywords if matched_keywords else None,
                'number_of_positions_last_30': str(job['company_30d_postings_count']),
                'hr_contact_name': job['hr_contact_name'],
                'hr_contact_title': job['hr_contact_title'],
                'hr_contact_email': job['hr_contact_email'],
                'hr_contact_linkedin': job['hr_contact_linkedin'],
                'primary_contact': job['hr_contact_name'],
                'created_at': job['date_published'].isoformat() if job['date_published'] else None,
                'date_processed': job['populated_at'].isoformat() if job['populated_at'] else None,
            })

        # Get total count
        cursor.execute("SELECT COUNT(*) FROM qualified_indeed_jobs WHERE score >= %s", (min_score,))
        total_count = cursor.fetchone()['count']

        cursor.close()
        conn.close()

        logger.info(f"Fetched {len(result)} jobs (total: {total_count})")

        return jsonify({
            'data': result,
            'total': total_count,
            'limit': limit,
            'offset': offset
        })

    except Exception as e:
        logger.error(f"Error fetching jobs: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/jobs/qualified', methods=['GET'])
def get_qualified_jobs():
    """
    Get raw qualified jobs data (alternative endpoint).

    Query parameters same as /api/jobs/summary
    """
    limit = request.args.get('limit', 100, type=int)
    offset = request.args.get('offset', 0, type=int)
    min_score = request.args.get('min_score', 80, type=int)

    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)

        query = """
            SELECT *
            FROM qualified_indeed_jobs
            WHERE score >= %s
            ORDER BY score DESC, populated_at DESC
            LIMIT %s OFFSET %s
        """

        cursor.execute(query, (min_score, limit, offset))
        jobs = cursor.fetchall()

        # Convert datetime to ISO format
        result = []
        for job in jobs:
            job_dict = dict(job)
            if job_dict.get('date_published'):
                job_dict['date_published'] = job_dict['date_published'].isoformat()
            if job_dict.get('populated_at'):
                job_dict['populated_at'] = job_dict['populated_at'].isoformat()
            result.append(job_dict)

        cursor.close()
        conn.close()

        logger.info(f"Fetched {len(result)} qualified jobs")

        return jsonify({
            'data': result,
            'total': len(result)
        })

    except Exception as e:
        logger.error(f"Error fetching qualified jobs: {e}")
        return jsonify({'error': str(e)}), 500


if __name__ == '__main__':
    # Use PORT env var for Heroku, fallback to 5001 for local development
    port = int(os.getenv('PORT', os.getenv('API_PORT', 5001)))
    debug_mode = os.getenv('FLASK_ENV', 'development') == 'development'
    logger.info(f"Starting API server on port {port} (debug={debug_mode})")
    app.run(host='0.0.0.0', port=port, debug=debug_mode)
