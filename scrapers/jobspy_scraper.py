# scrapers/jobspy_scraper.py

"""
A unified scraper that leverages the 'jobspy' library.

This module provides the JobSpyScraper class, which acts as an adapter for the
'python-jobspy' third-party library. It concurrently fetches job listings
from multiple major job boards (Indeed, LinkedIn, Google, Naukri) and transforms
the resulting pandas DataFrame into the application's standard data format.
"""
from typing import List, Dict, Any

import pandas as pd
from loguru import logger

try:
    from jobspy import scrape_jobs
except ImportError:
    print(
        "FATAL: The 'python-jobspy' library is not installed. "
        "Please run 'pip install python-jobspy'."
    )
    scrape_jobs = None


# pylint: disable=too-few-public-methods
class JobSpyScraper:
    """
    An adapter for the 'jobspy' library to fetch jobs from multiple sites.
    """
    SOURCE_NAME = "JobSpy"
    SUPPORTED_SITES = ["indeed", "linkedin", "google", "naukri"]

    def __init__(self, keyword: str, location: str, **kwargs: Any):
        if not scrape_jobs:
            raise RuntimeError("JobSpy library is not available.")

        self.keyword = keyword
        self.location = location
        self.results_wanted = kwargs.get('results_wanted', 25)
        self.country_indeed = kwargs.get('country_indeed', 'India')
        self.log = logger.bind(source=self.SOURCE_NAME)
        self.log.info(
            f"Initialized for keyword: '{self.keyword}', location: '{self.location}'"
        )

    def _extract_salary_from_row(self, row: pd.Series) -> str:
        """Safely extracts and formats the salary range from a DataFrame row."""
        min_salary, max_salary = row.get('min_amount'), row.get('max_amount')
        currency = row.get('currency', '')

        if pd.notna(min_salary) and pd.notna(max_salary):
            return f"{min_salary:,.0f} - {max_salary:,.0f} {currency}".strip()
        if pd.notna(min_salary):
            return f"{min_salary:,.0f} {currency}".strip()

        return "Not Disclosed"

    def _transform_row_to_dict(self, row: pd.Series) -> Dict[str, Any]:
        """Transforms a single pandas DataFrame row into a standardized dictionary."""
        description_raw = row.get('description')
        description = str(description_raw).strip() if pd.notna(description_raw) else "No description"

        skills_raw = row.get('skills')
        skills = str(skills_raw) if pd.notna(skills_raw) else "Not Disclosed"

        return {
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
            "salary_range": self._extract_salary_from_row(row),
        }

    def _transform_dataframe_to_dicts(self, jobs_df: pd.DataFrame) -> List[Dict[str, Any]]:
        """Converts the entire jobspy DataFrame to a list of standard dictionaries."""
        if jobs_df.empty:
            return []
        return [self._transform_row_to_dict(row) for _, row in jobs_df.iterrows()]

    def scrape(self) -> List[Dict[str, Any]]:
        """
        Executes the job search using the jobspy library and transforms the output.

        Returns:
            A list of scraped and standardized job dictionaries.
        """
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
                verbose=0  # Reduce console spam from the underlying library
            )

            if jobs_df is None or jobs_df.empty:
                self.log.warning("JobSpy returned no data for this query.")
                return []

            self.log.success(
                f"JobSpy successfully fetched {len(jobs_df)} "
                f"raw listings for '{self.keyword}'."
            )
            return self._transform_dataframe_to_dicts(jobs_df)

        # Justification: The jobspy library can raise a wide variety of unexpected
        # exceptions (network, parsing, etc.). Catching a broad exception ensures
        # that a failure in this scraper doesn't crash the main application.
        except Exception as e:  # pylint: disable=broad-exception-caught
            self.log.error(
                f"An unexpected error occurred while running JobSpy for '{self.keyword}'. "
                f"Error: {e}"
            )
            return []