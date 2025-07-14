# scrapers/jobspy_scraper.py

import pandas as pd
from loguru import logger
from typing import List, Dict, Any

try:
    from jobspy import scrape_jobs
except ImportError:
    print("FATAL: The 'python-jobspy' library is not installed. Please run 'pip install python-jobspy'.")
    scrape_jobs = None

class JobSpyScraper:
    """
    A unified scraper that leverages the 'jobspy' library to concurrently fetch
    job listings from multiple major job boards: Indeed, LinkedIn, Google, and Naukri.
    """
    SOURCE_NAME = "JobSpy"
    SUPPORTED_SITES = ["indeed", "linkedin", "google", "naukri"]

    def __init__(self, keyword: str, location: str, **kwargs):
        if not scrape_jobs:
            raise RuntimeError("JobSpy library is not available.")
        
        self.keyword = keyword
        self.location = location
        self.results_wanted = kwargs.get('results_wanted', 25) # Default to a reasonable 25
        self.country_indeed = kwargs.get('country_indeed', 'India')
        self.log = logger.bind(source=f"{self.SOURCE_NAME}")
        self.log.info(f"Initialized for keyword: '{self.keyword}', location: '{self.location}'")

    def _transform_dataframe_to_dicts(self, jobs_df: pd.DataFrame) -> List[Dict[str, Any]]:
        if jobs_df.empty:
            return []

        standardized_jobs = []
        for _, row in jobs_df.iterrows():
            # **FIXED**: Robustly handle potentially missing string data
            description_raw = row.get('description')
            description = str(description_raw).strip() if pd.notna(description_raw) else "No description provided."

            skills_raw = row.get('skills')
            skills = str(skills_raw) if pd.notna(skills_raw) else "Not Disclosed"
            
            # Safely get salary info
            min_salary, max_salary = row.get('min_amount'), row.get('max_amount')
            salary_range = "Not Disclosed"
            if pd.notna(min_salary) and pd.notna(max_salary):
                salary_range = f"{min_salary:,.0f} - {max_salary:,.0f} {row.get('currency', '')}".strip()
            elif pd.notna(min_salary):
                salary_range = f"{min_salary:,.0f} {row.get('currency', '')}".strip()

            standardized_job = {
                "source_platform": row.get('site', self.SOURCE_NAME),
                "job_id": str(row.get('job_url_id', 'N/A')),
                "job_title": str(row.get('title', 'No Title Provided')),
                "company_name": str(row.get('company', 'No Company Name')),
                "job_url": row.get('job_url', 'N/A'),
                "location": row.get('location', 'Not Disclosed'),
                "date_posted": row.get('date_posted', 'N/A'),
                "description": description,
                "skills": skills,
                "experience_required": row.get('job_type', 'Not Disclosed'),
                "salary_range": salary_range,
            }
            standardized_jobs.append(standardized_job)
        
        return standardized_jobs

    def scrape(self) -> List[Dict[str, Any]]:
        self.log.info(f"Launching JobSpy to search for '{self.keyword}'...")
        google_search_query = f"{self.keyword} jobs in {self.location}"

        try:
            jobs_df = scrape_jobs(
                site_name=self.SUPPORTED_SITES,
                search_term=self.keyword,
                location=self.location,
                results_wanted=self.results_wanted,
                country_indeed=self.country_indeed,
                google_search_term=google_search_query,
                # Adding verbosity control to reduce console spam from jobspy
                verbose=0 
            )

            if jobs_df is None or jobs_df.empty:
                self.log.warning("JobSpy returned no data for this query.")
                return []

            self.log.success(f"JobSpy successfully fetched {len(jobs_df)} raw listings for '{self.keyword}'.")
            return self._transform_dataframe_to_dicts(jobs_df)

        except Exception as e:
            self.log.error(f"An unexpected error occurred while running JobSpy for '{self.keyword}'. Error: {e}")
            return []