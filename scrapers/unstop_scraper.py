# scrapers/unstop_scraper.py

"""
Scraper for fetching job listings from Unstop.com's public API.

This module contains the UnstopScraper class, which is responsible for
querying the Unstop API with a search keyword, paginating through the
results, and transforming the data into the application's standard format.
"""
import time
from datetime import datetime
from typing import List, Dict, Any, Optional

import requests
from loguru import logger


# pylint: disable=too-few-public-methods
class UnstopScraper:
    """A scraper for Unstop.com that targets its search API."""
    SOURCE_NAME = "Unstop.com"
    API_ENDPOINT = "https://unstop.com/api/public/opportunity/search-result"
    JOB_URL_PREFIX = "https://unstop.com"
    USER_AGENT = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
    )
    HEADERS = {
        "User-Agent": USER_AGENT,
        "Referer": f"{JOB_URL_PREFIX}/jobs"
    }

    def __init__(self, keyword: str, **kwargs: Any):
        self.keyword = keyword
        self.max_pages: Optional[int] = kwargs.get('max_pages')
        self.log = logger.bind(source=self.SOURCE_NAME)
        self.log.info(f"Initialized for keyword: '{self.keyword}'")

    def _fetch_page(self, page_number: int) -> Optional[Dict[str, Any]]:
        """Fetches a single page of results from the Unstop API."""
        params = {
            'opportunity': 'jobs',
            'page': page_number,
            'per_page': 20,
            'oppstatus': 'recent',
            'searchTerm': self.keyword
        }
        try:
            response = requests.get(
                self.API_ENDPOINT, params=params, headers=self.HEADERS, timeout=20
            )
            response.raise_for_status()
            return response.json()
        except requests.exceptions.JSONDecodeError:
            # The response was not valid JSON, which is a critical error.
            # Logging the raw text is more useful than the exception object itself.
            self.log.error(
                f"Failed to decode JSON from API on page {page_number}. Content: "
                f"{response.text[:200]}"
            )
            return None
        except requests.exceptions.RequestException as e:
            self.log.error(f"Network request failed for page {page_number}. Error: {e}")
            return None

    def _extract_salary(self, job_detail: Dict[str, Any]) -> str:
        """Extracts the salary range from the job detail object."""
        is_disclosed = not job_detail.get('not_disclosed', True)
        min_salary = job_detail.get('min_salary')

        if is_disclosed and min_salary is not None:
            min_sal_str = f"₹{int(min_salary):,}"
            max_sal_str = f"₹{int(job_detail.get('max_salary', 0)):,}"
            return f"{min_sal_str} - {max_sal_str}"
        return "Not Disclosed"

    def _extract_date_posted(self, raw_job: Dict[str, Any]) -> str:
        """Extracts and formats the posting date from the raw job object."""
        if post_date_str := raw_job.get('approved_date'):
            try:
                return datetime.fromisoformat(post_date_str).strftime('%Y-%m-%d')
            except (TypeError, ValueError):
                job_id = raw_job.get('id', 'N/A')
                self.log.warning(f"Could not parse date '{post_date_str}' for job ID: {job_id}")
        return "Not Disclosed"

    def _transform_job(self, raw_job: Dict[str, Any]) -> Dict[str, Any]:
        """
        Transforms a single raw job dictionary into the standard format by calling
        specialized helper methods for data extraction.
        """
        job_detail = raw_job.get('jobDetail') or {}
        org_detail = raw_job.get('organisation') or {}
        seo_list = raw_job.get('seo_details', [])
        seo_detail = seo_list[0] if seo_list else {}

        # Extract location and experience
        locations = job_detail.get('locations', [])
        location_str = ", ".join(loc for loc in locations if loc) or "Not Disclosed"
        filters = raw_job.get('filters', [])
        experience_required = ", ".join(
            f.get('name', '') for f in filters if f.get('name')
        ) or "Not Disclosed"

        # Final Assembly
        return {
            "source_platform": self.SOURCE_NAME,
            "job_id": str(raw_job.get('id', 'N/A')),
            "job_title": raw_job.get('title', 'No Title Provided'),
            "company_name": org_detail.get('name', 'No Company Name'),
            "job_url": f"{self.JOB_URL_PREFIX}{raw_job.get('seo_url', '')}",
            "location": location_str,
            "date_posted": self._extract_date_posted(raw_job),
            "description": seo_detail.get('description', 'No description provided.').strip(),
            "skills": "Not Disclosed",  # API does not provide a clear skills list
            "experience_required": experience_required,
            "salary_range": self._extract_salary(job_detail),
        }

    def scrape(self) -> List[Dict[str, Any]]:
        """
        Executes the scraping process, paginating through all available results
        or until the max_pages limit is reached.
        """
        self.log.info("Starting scrape...")
        all_jobs: List[Dict[str, Any]] = []
        page_num = 1
        while True:
            if self.max_pages and page_num > self.max_pages:
                self.log.info(f"Reached user-defined page limit of {self.max_pages}.")
                break

            self.log.debug(f"Fetching page {page_num}...")
            raw_data = self._fetch_page(page_num)
            if not raw_data:
                break  # Error occurred in _fetch_page

            job_listings = raw_data.get('data', {}).get('data', [])
            if not job_listings:
                self.log.info("No more jobs found in API response. Ending scrape.")
                break

            for raw_job in job_listings:
                try:
                    standard_job = self._transform_job(raw_job)
                    all_jobs.append(standard_job)
                # Justification: A broad exception is caught here because a single
                # malformed job from the API should be skipped without crashing
                # the entire scraping process for the page.
                except Exception as e:  # pylint: disable=broad-exception-caught
                    job_id = raw_job.get('id', 'UNKNOWN')
                    self.log.warning(
                        f"Could not transform job ID {job_id}. Skipping. Error: {e}"
                    )
            page_num += 1
            time.sleep(0.5)  # Respectful delay

        self.log.success(f"Scrape complete. Found {len(all_jobs)} jobs.")
        return all_jobs