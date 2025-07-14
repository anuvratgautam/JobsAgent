# core/data_processor.py

import pandas as pd
from loguru import logger
from pathlib import Path
from typing import List, Dict, Any
from datetime import datetime

class DataProcessor:
    """
    Handles the processing and storage of scraped job data.
    """
    _COLUMN_ORDER = [
        "source_platform", "job_title", "company_name", "location",
        "date_posted", "experience_required", "salary_range", "skills",
        "description", "job_url"
    ]

    def __init__(self, output_dir: str = "scraped_data"):
        self.output_path = Path(output_dir)
        self.log = logger.bind(source="DataProcessor")
        try:
            self.output_path.mkdir(parents=True, exist_ok=True)
            self.log.info(f"Output directory initialized at: '{self.output_path.resolve()}'")
        except Exception as e:
            self.log.critical(f"Failed to create output directory at '{output_dir}'. Error: {e}")
            raise

    def _remove_duplicates(self, jobs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Removes duplicate job listings based on the 'job_url'. This is a more
        reliable unique identifier than a platform-specific job_id.
        """
        if not jobs:
            return []
            
        seen_urls = set()
        unique_jobs = []
        duplicates_found = 0
        for job in jobs:
            # The job URL is the most reliable unique identifier.
            identifier = job.get("job_url")
            
            if not identifier or identifier == 'N/A':
                self.log.warning(f"Job with title '{job.get('job_title')}' has no URL. Cannot de-duplicate.")
                unique_jobs.append(job)
                continue

            if identifier not in seen_urls:
                seen_urls.add(identifier)
                unique_jobs.append(job)
            else:
                duplicates_found += 1
        
        if duplicates_found > 0:
            self.log.info(f"Removed {duplicates_found} duplicate job listings.")
            
        return unique_jobs

    def save_to_excel(self, all_jobs: List[Dict[str, Any]]) -> str | None:
        if not all_jobs:
            self.log.warning("Received an empty list of jobs. Nothing to save.")
            return None

        self.log.info(f"Starting data processing for {len(all_jobs)} total collected jobs.")

        unique_jobs = self._remove_duplicates(all_jobs)
        self.log.success(f"Processing {len(unique_jobs)} unique job listings.")

        try:
            df = pd.DataFrame(unique_jobs)
            
            # Drop the 'job_id' column if it exists as it's not needed in the final output
            if 'job_id' in df.columns:
                df = df.drop(columns=['job_id'])

            for col in self._COLUMN_ORDER:
                if col not in df.columns:
                    df[col] = "N/A"
            
            df = df[self._COLUMN_ORDER]
            
        except Exception as e:
            self.log.error(f"Failed to create or process DataFrame. Error: {e}")
            return None

        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        file_name = f"job_listings_{timestamp}.xlsx"
        full_file_path = self.output_path / file_name

        try:
            self.log.info(f"Attempting to save data to '{full_file_path}'...")
            df.to_excel(full_file_path, index=False, engine='openpyxl')
            self.log.success(f"Successfully saved {len(unique_jobs)} jobs to '{full_file_path.resolve()}'")
            return str(full_file_path.resolve())
        except Exception as e:
            self.log.error(f"Failed to save Excel file. Please ensure 'openpyxl' is installed (`pip install openpyxl`). Error: {e}")
            return None