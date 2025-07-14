# scrapers/instahyre_scraper.py
# (Only __init__ and scrape methods are shown for brevity)
import time, os, json, requests
from loguru import logger

class InstahyreScraper:
    SOURCE_NAME = "Instahyre.com"
    BASE_URL = "https://www.instahyre.com/api/v1/job_search"
    API_PARAMS = { 'company_size': '0', 'isLandingPage': 'true', 'job_type': '0', 'limit': '20' }

    # **IMPROVED**: Added max_pages parameter
    def __init__(self, job_function_id: int, **kwargs):
        if not job_function_id:
            raise ValueError("Job function ID cannot be empty.")

        self.job_function_id = job_function_id
        self.max_pages = kwargs.get('max_pages') # Get the page limit
        self.log = logger.bind(source=self.SOURCE_NAME)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
            "Accept": "application/json"
        })
        self.log.info(f"Initialized for job function ID: {self.job_function_id}.")

    def _transform_job(self, raw_job: dict) -> dict:
        employer_info = raw_job.get('employer', {})
        return {
            "source_platform": self.SOURCE_NAME, "job_id": str(raw_job.get('id', 'N/A')),
            "job_title": raw_job.get('title', 'No Title'), "company_name": employer_info.get('company_name', 'No Company Name'),
            "job_url": raw_job.get('public_url', 'N/A'), "location": raw_job.get('locations', 'Not Disclosed'),
            "date_posted": "N/A", "description": employer_info.get('instahyre_note', 'No summary provided.').strip(),
            "skills": ", ".join(raw_job.get('keywords', [])), "experience_required": "Not Disclosed",
            "salary_range": "Not Disclosed",
        }

    def scrape(self) -> list[dict]:
        self.log.info(f"Starting scrape for job function ID '{self.job_function_id}'...")
        all_jobs, offset, page_num, retries, max_retries = [], 0, 1, 0, 3

        while True:
            # **IMPROVED**: Check for user-defined page limit
            if self.max_pages and page_num > self.max_pages:
                self.log.info(f"Reached user-defined page limit of {self.max_pages}.")
                break
            if retries >= max_retries:
                self.log.error(f"Exceeded max retries ({max_retries}) on page {page_num}. Aborting scrape.")
                break
            # ... (rest of the scrape method is the same)
            params = self.API_PARAMS.copy()
            params['job_functions'] = str(self.job_function_id)
            params['offset'] = str(offset)
            try:
                self.log.info(f"Requesting page {page_num} (offset {offset})...")
                response = self.session.get(self.BASE_URL, params=params, timeout=30) # Increased timeout
                response.raise_for_status()
                retries = 0
                data = response.json()
                job_listings = data.get("objects", [])
                if not job_listings:
                    self.log.info("No more jobs found. Scrape complete.")
                    break
                self.log.success(f"Successfully fetched {len(job_listings)} jobs from page {page_num}.")
                all_jobs.extend(self._transform_job(job) for job in job_listings)
                offset += len(job_listings)
                page_num += 1
                time.sleep(2)
            except requests.exceptions.HTTPError as e:
                # ... (error handling is the same)
                break
            except (requests.exceptions.RequestException, json.JSONDecodeError) as e:
                self.log.error(f"A network or JSON error occurred: {e}. Stopping scrape.")
                break

        self.session.close()
        self.log.success(f"Scrape finished. Found {len(all_jobs)} jobs in total.")
        return all_jobs