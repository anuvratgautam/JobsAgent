# scrapers/unstop_scraper.py
# (Only the _transform_job method is shown for brevity)
import requests
import time
from loguru import logger
from datetime import datetime

class UnstopScraper:
    SOURCE_NAME = "Unstop.com"
    API_ENDPOINT = "https://unstop.com/api/public/opportunity/search-result"
    JOB_URL_PREFIX = "https://unstop.com"
    HEADERS = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
        "Referer": f"{JOB_URL_PREFIX}/jobs"
    }

    def __init__(self, keyword: str, **kwargs):
        self.keyword = keyword
        self.max_pages = kwargs.get('max_pages')
        self.log = logger.bind(source=self.SOURCE_NAME)
        self.log.info(f"Initialized for keyword: '{self.keyword}'")
    
    def _fetch_page(self, page_number: int) -> dict | None:
        params = {
            'opportunity': 'jobs', 'page': page_number, 'per_page': 20,
            'oppstatus': 'recent', 'searchTerm': self.keyword
        }
        try:
            response = requests.get(self.API_ENDPOINT, params=params, headers=self.HEADERS, timeout=20)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            self.log.error(f"Network request failed for page {page_number}. Error: {e}")
            return None
        except requests.exceptions.JSONDecodeError as e:
            self.log.error(f"Failed to decode JSON from API response on page {page_number}. Content: {response.text[:200]}")
            return None
            
    def _transform_job(self, raw_job: dict) -> dict:
        """
        Transforms a single raw job dictionary. Now more defensive against
        completely null nested objects from the API.
        """
        # **FIXED**: Default to an empty dictionary if the entire object is None
        job_detail = raw_job.get('jobDetail') or {}
        org_detail = raw_job.get('organisation') or {}
        
        seo_list = raw_job.get('seo_details', [])
        seo_detail = seo_list[0] if seo_list else {}

        # --- Salary Extraction ---
        salary_range = "Not Disclosed"
        if not job_detail.get('not_disclosed') and job_detail.get('min_salary') is not None:
            min_sal = f"₹{job_detail.get('min_salary', 0):,}"
            max_sal = f"₹{job_detail.get('max_salary', 0):,}"
            salary_range = f"{min_sal} - {max_sal}"

        # --- Date Posted Extraction ---
        date_posted = "Not Disclosed"
        if post_date_str := raw_job.get('approved_date'):
            try:
                date_posted = datetime.fromisoformat(post_date_str).strftime('%Y-%m-%d')
            except (TypeError, ValueError):
                self.log.warning(f"Could not parse date '{post_date_str}' for job ID: {raw_job.get('id')}")

        # --- Location & Experience Extraction ---
        locations = job_detail.get('locations', [])
        location_str = ", ".join(loc for loc in locations if loc) if locations else "Not Disclosed"
        filters = raw_job.get('filters', [])
        experience_required = ", ".join(f.get('name', '') for f in filters if f.get('name')) or "Not Disclosed"

        # --- Final Assembly ---
        standardized_job = {
            "source_platform": self.SOURCE_NAME,
            "job_id": str(raw_job.get('id', 'N/A')),
            "job_title": raw_job.get('title', 'No Title Provided'),
            "company_name": org_detail.get('name', 'No Company Name'),
            "job_url": f"{self.JOB_URL_PREFIX}{raw_job.get('seo_url', '')}",
            "location": location_str,
            "date_posted": date_posted,
            "description": seo_detail.get('description', 'No description provided.').strip(),
            "skills": "Not Disclosed",
            "experience_required": experience_required,
            "salary_range": salary_range,
        }
        return standardized_job

    def scrape(self) -> list[dict]:
        self.log.info("Starting scrape...")
        all_standardized_jobs, page_num = [], 1
        while True:
            if self.max_pages and page_num > self.max_pages:
                self.log.info(f"Reached user-defined page limit of {self.max_pages}.")
                break
            self.log.debug(f"Fetching page {page_num}...")
            raw_data = self._fetch_page(page_num)
            job_listings = raw_data.get('data', {}).get('data', []) if raw_data else []
            if not job_listings:
                self.log.info("No more jobs found in API response. Ending scrape.")
                break
            for raw_job in job_listings:
                try:
                    standard_job = self._transform_job(raw_job)
                    all_standardized_jobs.append(standard_job)
                except Exception as e:
                    job_id = raw_job.get('id', 'UNKNOWN')
                    self.log.warning(f"Could not transform job ID {job_id}. Skipping. Error: {e}")
            page_num += 1
            time.sleep(0.5)
        self.log.success(f"Scrape complete. Found {len(all_standardized_jobs)} jobs.")
        return all_standardized_jobs