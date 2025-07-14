# main.py

import sys
from pathlib import Path
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Any, Set

from loguru import logger

# --- Imports ---
from core import JobFinder, DataProcessor
from scrapers import JobSpyScraper, InstahyreScraper, UnstopScraper
try:
    from config import GOOGLE_API_KEY
    if not GOOGLE_API_KEY or GOOGLE_API_KEY == "YOUR_GOOGLE_API_KEY":
        print("FATAL: Google API Key is missing. Please set it in config.py")
        sys.exit(1)
except ImportError:
    print("FATAL: config.py file not found or GOOGLE_API_KEY is missing.")
    sys.exit(1)

# **NEW**: Set of valid countries for validation, derived from the jobspy error log.
VALID_COUNTRIES: Set[str] = {
    'argentina', 'australia', 'austria', 'bahrain', 'belgium', 'bulgaria', 'brazil',
    'canada', 'chile', 'china', 'colombia', 'costa rica', 'croatia', 'cyprus',
    'czech republic', 'czechia', 'denmark', 'ecuador', 'egypt', 'estonia', 'finland',
    'france', 'germany', 'greece', 'hong kong', 'hungary', 'india', 'indonesia',
    'ireland', 'israel', 'italy', 'japan', 'kuwait', 'latvia', 'lithuania', 'luxembourg',
    'malaysia', 'malta', 'mexico', 'morocco', 'netherlands', 'new zealand', 'nigeria',
    'norway', 'oman', 'pakistan', 'panama', 'peru', 'philippines', 'poland', 'portugal',
    'qatar', 'romania', 'saudi arabia', 'singapore', 'slovakia', 'slovenia',
    'south africa', 'south korea', 'spain', 'sweden', 'switzerland', 'taiwan',
    'thailand', 'türkiye', 'turkey', 'ukraine', 'united arab emirates', 'uk',
    'united kingdom', 'usa', 'us', 'united states', 'uruguay', 'venezuela', 'vietnam'
}


def setup_logging():
    Path("logs").mkdir(exist_ok=True)
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    log_file = Path("logs") / f"run_{timestamp}.log"
    log_format = "{time:YYYY-MM-DD HH:mm:ss.SSS} | {level: <8} | {extra[source]: <18} | {message}"
    logger.add(log_file, level="DEBUG", format=log_format, enqueue=True, backtrace=True, diagnose=True)
    logger.configure(extra={"source": "Orchestrator"})
    logger.info("--- Starting New Job Search Session ---")

def run_scraper_task(scraper_class, **kwargs) -> List[Dict[str, Any]]:
    # This function remains unchanged
    source_name = getattr(scraper_class, 'SOURCE_NAME', scraper_class.__name__)
    log = logger.bind(source=source_name)
    try:
        log.info(f"Initializing with args: {kwargs}")
        scraper_instance = scraper_class(**kwargs)
        results = scraper_instance.scrape()
        if results:
            log.success(f"Scrape successful. Found {len(results)} jobs.")
        else:
            log.warning("Scrape finished but found no jobs.")
        return results
    except Exception as e:
        log.error(f"A critical error occurred during scraping. Error: {e}", exc_info=True)
        return []

def main():
    logger.info("--- Step 1: Gathering User Input ---")
    resume_path_str = input("Enter the full path to your resume file (.pdf or .txt): ").strip().replace('"', '')
    resume_path = Path(resume_path_str)
    if not resume_path.is_file():
        logger.critical(f"Resume file not found at '{resume_path}'. Aborting.")
        return

    user_interests = input("Enter your primary job interests (e.g., machine learning, data science): ").strip()
    location = input("Enter the job location to search (e.g., Bengaluru, 'San Francisco, CA'): ").strip()

    # **FIXED**: Input validation loop for the country
    while True:
        country = input("Enter the country for Indeed/Naukri (e.g., India, USA) [Default: India]: ").strip().lower() or "india"
        if country in VALID_COUNTRIES:
            break
        else:
            print(f"'{country}' is not a valid country for JobSpy. Please try again.")
            print("Hint: Use names like 'united kingdom', 'usa', 'united arab emirates', etc.")

    try:
        results_per_site = int(input("Enter max jobs from Indeed/LinkedIn etc. per search term (e.g., 25) [Default: 25]: ").strip() or "25")
    except ValueError:
        logger.warning("Invalid number. Defaulting to 25 results per site.")
        results_per_site = 25
        
    try:
        pages_for_others = int(input("Enter max pages for Unstop/Instahyre per search term (e.g., 5) [Default: 5]: ").strip() or "5")
    except ValueError:
        logger.warning("Invalid number. Defaulting to 5 pages.")
        pages_for_others = 5
    
    # Step 2: Getting Job Titles (No changes needed)
    logger.info("--- Step 2: Getting Job Title Suggestions from AI ---")
    try:
        job_finder = JobFinder(api_key=GOOGLE_API_KEY)
        job_titles = job_finder.get_job_titles(resume_path=str(resume_path), user_interests=user_interests)
    except Exception as e:
        logger.critical(f"Failed to get job titles from AI. Aborting. Error: {e}")
        return
    if not job_titles:
        logger.error("AI could not suggest any job titles. Aborting.")
        return
    logger.success(f"AI suggested the following job titles: {job_titles}")

    # Step 3: Launching Scrapers (No changes needed)
    logger.info("--- Step 3: Launching All Scrapers in Parallel ---")
    all_scraped_jobs = []
    with ThreadPoolExecutor(max_workers=12) as executor: # Slightly increased workers
        future_to_scraper = {}
        for title in job_titles:
            future_to_scraper[executor.submit(
                run_scraper_task, JobSpyScraper, keyword=title, location=location, 
                results_wanted=results_per_site, country_indeed=country
            )] = "JobSpy"
            future_to_scraper[executor.submit(
                run_scraper_task, UnstopScraper, keyword=title, max_pages=pages_for_others
            )] = "Unstop"

        future_to_scraper[executor.submit(
            run_scraper_task, InstahyreScraper, job_function_id=9, max_pages=pages_for_others
        )] = "Instahyre"
        
        logger.info(f"Submitted {len(future_to_scraper)} scraping tasks to the executor.")

        for future in as_completed(future_to_scraper):
            scraper_name = future_to_scraper[future]
            try:
                results = future.result()
                if results:
                    all_scraped_jobs.extend(results)
            except Exception as e:
                logger.error(f"Task for {scraper_name} generated an exception: {e}")
    
    logger.success(f"All scrapers finished. Total jobs collected (before de-duplication): {len(all_scraped_jobs)}")

    # Step 4: Processing Data (No changes needed)
    if all_scraped_jobs:
        logger.info("--- Step 4: Processing and Saving Data ---")
        processor = DataProcessor(output_dir="scraped_data")
        output_file = processor.save_to_excel(all_scraped_jobs)
        if output_file:
            logger.info(f"Process complete. Final report saved to: {output_file}")
        else:
            logger.error("Data processing failed. No output file was generated.")
    else:
        logger.warning("No jobs were collected from any source. Skipping data processing.")

if __name__ == "__main__":
    setup_logging()
    try:
        main()
    except Exception as e:
        logger.exception("An unhandled exception occurred in the main orchestrator.")
    finally:
        logger.info("--- Job Search Session Finished ---\n")