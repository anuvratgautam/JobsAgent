# scrapers/instahyre_scraper.py

"""
Scraper for fetching job listings from Instahyre.com's private API.

This module contains the InstahyreScraper class, which is responsible for
querying the Instahyre API for a specific job function, parsing the JSON
response, and transforming the data into the application's standard job
dictionary format.
"""
import json
import time
from typing import List, Dict, Any, Optional

import requests
from loguru import logger


# pylint: disable=too-few-public-methods
class InstahyreScraper:
    """
    A scraper for Instahyre.com that targets its internal API.

    This class manages a requests session, handles pagination, and transforms
    the API data into a standardized format.
    """
    SOURCE_NAME = "Instahyre.com"
    BASE_URL = "https://www.instahyre.com/api/v1/job_search"
    API_PARAMS = {'company_size': '0', 'isLandingPage': 'true', 'job_type': '0', 'limit': '20'}
    USER_AGENT = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
    )

    def __init__(self, job_function_id: int, **kwargs: Any):
        if not job_function_id:
            raise ValueError("Job function ID cannot be empty.")

        self.job_function_id = job_function_id
        self.max_pages: Optional[int] = kwargs.get('max_pages')
        self.log = logger.bind(source=self.SOURCE_NAME)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": self.USER_AGENT,
            "Accept": "application/json"
        })
        self.log.info(f"Initialized for job function ID: {self.job_function_id}.")

    def _transform_job(self, raw_job: Dict[str, Any]) -> Dict[str, Any]:
        """
        Transforms a single raw job dictionary from the API into the standard format.

        Args:
            raw_job: A dictionary representing a single job from the Instahyre API.

        Returns:
            A standardized job dictionary.
        """
        employer_info = raw_job.get('employer', {})
        description_note = employer_info.get('instahyre_note', 'No summary provided.')

        return {
            "source_platform": self.SOURCE_NAME,
            "job_id": str(raw_job.get('id', 'N/A')),
            "job_title": raw_job.get('title', 'No Title'),
            "company_name": employer_info.get('company_name', 'No Company Name'),
            "job_url": raw_job.get('public_url', 'N/A'),
            "location": raw_job.get('locations', 'Not Disclosed'),
            "date_posted": "N/A",
            "description": description_note.strip(),
            "skills": ", ".join(raw_job.get('keywords', [])),
            "experience_required": "Not Disclosed",
            "salary_range": "Not Disclosed",
        }

    def scrape(self) -> List[Dict[str, Any]]:
        """
        Executes the scraping process, paginating through results until no more are found
        or the max_pages limit is reached.

        Returns:
            A list of all scraped and transformed job dictionaries.
        """
        self.log.info(f"Starting scrape for job function ID '{self.job_function_id}'...")
        all_jobs: List[Dict[str, Any]] = []
        offset, page_num, retries, max_retries = 0, 1, 0, 3

        while True:
            if self.max_pages and page_num > self.max_pages:
                self.log.info(f"Reached user-defined page limit of {self.max_pages}.")
                break
            if retries >= max_retries:
                self.log.error(f"Exceeded max retries ({max_retries}) on page {page_num}.")
                break

            params = self.API_PARAMS.copy()
            params['job_functions'] = str(self.job_function_id)
            params['offset'] = str(offset)
            try:
                self.log.info(f"Requesting page {page_num} (offset {offset})...")
                response = self.session.get(self.BASE_URL, params=params, timeout=30)
                response.raise_for_status()
                retries = 0  # Reset retries on success
                data = response.json()
                job_listings = data.get("objects", [])
                if not job_listings:
                    self.log.info("No more jobs found. Scrape complete.")
                    break

                processed_count = len(job_listings)
                self.log.success(f"Fetched {processed_count} jobs from page {page_num}.")
                all_jobs.extend(self._transform_job(job) for job in job_listings)
                offset += processed_count
                page_num += 1
                time.sleep(2)  # Respectful delay between requests

            except requests.exceptions.HTTPError as e:
                self.log.error(f"HTTP error on page {page_num}: {e}. Aborting scrape.")
                break
            except (requests.exceptions.RequestException, json.JSONDecodeError) as e:
                self.log.error(f"A network or JSON error occurred: {e}. Aborting scrape.")
                break

        self.session.close()
        self.log.success(f"Scrape finished. Found {len(all_jobs)} jobs in total.")
        return all_jobs