# core/data_processor.py

"""
Handles the processing and storage of scraped job data.

This module contains the DataProcessor class, which is responsible for
taking a list of raw job dictionaries, de-duplicating them based on their
URL, standardizing the columns, and saving the clean data to a timestamped
Excel file. It also includes a cleanup utility to remove old data files.
"""
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Any, Optional

import pandas as pd
from loguru import logger


# pylint: disable=too-few-public-methods
class DataProcessor:
    """
    Cleans, de-duplicates, and saves job data to an Excel file.
    Also manages the output directory by cleaning up old files.
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
            self.log.info(f"Output directory set to: '{self.output_path.resolve()}'")
        # Justification: Directory creation can fail for many OS-level reasons
        # (e.g., permissions). Catching Exception is a safeguard for startup.
        except Exception as e:  # pylint: disable=broad-exception-caught
            self.log.critical(
                f"Failed to create output directory at '{output_dir}'. Error: {e}"
            )
            raise

    # --- NEW METHOD ---
    def cleanup_old_files(self, days_old: int = 7):
        """
        Deletes .xlsx files in the output directory older than a specified number of days.
        """
        self.log.info(f"Running cleanup task for files older than {days_old} days...")
        if not self.output_path.exists():
            self.log.warning("Output directory does not exist. Skipping cleanup.")
            return

        cutoff_time = datetime.now() - timedelta(days=days_old)
        files_deleted = 0
        for file_path in self.output_path.glob("*.xlsx"):
            try:
                file_mod_time_ts = file_path.stat().st_mtime
                file_mod_time = datetime.fromtimestamp(file_mod_time_ts)

                if file_mod_time < cutoff_time:
                    self.log.info(
                        f"Deleting old file: {file_path.name} "
                        f"(last modified on {file_mod_time.date()})"
                    )
                    file_path.unlink()  # This deletes the file
                    files_deleted += 1
            except OSError as e:
                self.log.error(f"Error deleting file {file_path.name}: {e}")

        if files_deleted > 0:
            self.log.success(f"Cleanup complete. Deleted {files_deleted} old file(s).")
        else:
            self.log.info("Cleanup complete. No old files to delete.")


    def _remove_duplicates(self, jobs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Removes duplicate job listings based on the 'job_url'.
        """
        # ... (rest of the method is unchanged)
        if not jobs:
            return []

        seen_urls = set()
        unique_jobs = []
        duplicates_found = 0
        for job in jobs:
            identifier = job.get("job_url")

            if not identifier or identifier == 'N/A':
                self.log.warning(
                    f"Job '{job.get('job_title')}' has no URL. Cannot de-duplicate."
                )
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


    def save_to_excel(self, all_jobs: List[Dict[str, Any]]) -> Optional[str]:
        """
        Processes a list of jobs and saves the unique ones to a .xlsx file.
        """
        # ... (rest of the method is unchanged)
        if not all_jobs:
            self.log.warning("Received an empty list of jobs. Nothing to save.")
            return None

        self.log.info(f"Processing {len(all_jobs)} total collected jobs.")

        unique_jobs = self._remove_duplicates(all_jobs)
        self.log.success(f"Found {len(unique_jobs)} unique job listings to save.")

        try:
            df = pd.DataFrame(unique_jobs)
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
            self.log.success(
                f"Successfully saved {len(unique_jobs)} jobs to '{full_file_path.resolve()}'"
            )
            return str(full_file_path.resolve())
        except Exception as e:
            self.log.error(
                "Failed to save Excel file. Ensure 'openpyxl' is installed "
                f"(`pip install openpyxl`). Error: {e}"
            )
            return None