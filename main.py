# main.py

"""
Main orchestrator for the AI-powered Job Search application.

This script manages the end-to-end process of:
1. Gathering user input (resume, interests, location).
2. Using an AI model to suggest relevant job titles.
3. Allowing the user to refine the list of job titles.
4. Concurrently scraping multiple job boards for those titles.
5. Processing the collected data, de-duplicating it, and saving it to an Excel file.
"""

import sys
from pathlib import Path
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Any, Set, Tuple

from loguru import logger

# --- Local Imports ---
from core import JobFinder, DataProcessor
from scrapers import JobSpyScraper, InstahyreScraper, UnstopScraper

# Attempt to import the essential API key
try:
    from config import GOOGLE_API_KEY
    if not GOOGLE_API_KEY or GOOGLE_API_KEY == "YOUR_GOOGLE_API_KEY":
        print("FATAL: Google API Key is missing. Please set it in config.py")
        sys.exit(1)
except ImportError:
    print("FATAL: config.py file not found or GOOGLE_API_KEY is missing.")
    sys.exit(1)

# A set of valid countries for JobSpy validation to prevent runtime errors.
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
    """Configures the Loguru logger for the application session."""
    Path("logs").mkdir(exist_ok=True)
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    log_file = Path("logs") / f"run_{timestamp}.log"
    log_format = (
        "{time:YYYY-MM-DD HH:mm:ss.SSS} | {level: <8} | "
        "{extra[source]: <18} | {message}"
    )
    logger.add(
        log_file, level="DEBUG", format=log_format,
        enqueue=True, backtrace=True, diagnose=True
    )
    logger.configure(extra={"source": "Orchestrator"})
    logger.info("--- Starting New Job Search Session ---")


def run_scraper_task(scraper_class, **kwargs) -> List[Dict[str, Any]]:
    """
    Initializes and runs a single scraper instance, handling its lifecycle and errors.

    Args:
        scraper_class: The scraper class to instantiate.
        **kwargs: Arguments to pass to the scraper's constructor.

    Returns:
        A list of job dictionaries, or an empty list if an error occurs.
    """
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
    # Justification: A broad exception is caught here because any failure within a
    # specific scraper (network, parsing, etc.) should be logged but should not
    # crash the entire multi-threaded application.
    except Exception as e: # pylint: disable=broad-exception-caught
        log.error(f"A critical error occurred during scraping. Error: {e}", exc_info=True)
        return []


def get_user_settings() -> Dict[str, Any]:
    """
    Gathers and validates all necessary inputs from the user.

    Returns:
        A dictionary containing the user's specified settings.
    """
    settings = {}
    resume_path_str = input(
        "Enter the full path to your resume file (.pdf or .txt): "
    ).strip().replace('"', '')
    settings['resume_path'] = Path(resume_path_str)
    if not settings['resume_path'].is_file():
        logger.critical(f"Resume file not found at '{settings['resume_path']}'. Aborting.")
        sys.exit(1)

    settings['user_interests'] = input(
        "Enter your primary job interests (e.g., machine learning): "
    ).strip()
    settings['location'] = input(
        "Enter the job location (e.g., Bengaluru, 'San Francisco, CA'): "
    ).strip()

    while True:
        country = input(
            "Enter the country for Indeed/Naukri (e.g., India, USA) [Default: India]: "
        ).strip().lower() or "india"
        if country in VALID_COUNTRIES:
            settings['country'] = country
            break
        print(f"'{country}' is not a valid country. Please try again.")
        print("Hint: Use names like 'united kingdom', 'usa', 'united arab emirates'.")

    try:
        settings['results_per_site'] = int(input(
            "Max jobs from Indeed/LinkedIn per search (e.g., 25) [Default: 25]: "
        ).strip() or "25")
    except ValueError:
        logger.warning("Invalid number. Defaulting to 25 results per site.")
        settings['results_per_site'] = 25

    try:
        settings['pages_for_others'] = int(input(
            "Max pages for Unstop/Instahyre per search (e.g., 5) [Default: 5]: "
        ).strip() or "5")
    except ValueError:
        logger.warning("Invalid number. Defaulting to 5 pages.")
        settings['pages_for_others'] = 5

    return settings


def get_ai_job_titles(settings: Dict[str, Any]) -> List[str]:
    """
    Uses the JobFinder to get AI-suggested job titles based on user settings.
    """
    logger.info("--- Step 2: Getting Job Title Suggestions from AI ---")
    try:
        job_finder = JobFinder(api_key=GOOGLE_API_KEY)
        titles = job_finder.get_job_titles(
            resume_path=str(settings['resume_path']),
            user_interests=settings['user_interests']
        )
        if not titles:
            logger.error("AI could not suggest any job titles. Aborting.")
            sys.exit(1)
        logger.success(f"AI suggested the following job titles: {titles}")
        return titles
    # Justification: A broad exception is caught because a failure in the AI API
    # (network, config, etc.) is a critical, show-stopping error.
    except Exception as e: # pylint: disable=broad-exception-caught
        logger.critical(f"Failed to get job titles from AI. Aborting. Error: {e}")
        sys.exit(1)


def refine_job_titles_interactively(initial_titles: List[str]) -> List[str]:
    """
    Allows the user to interactively select, add, and refine the job title list.
    """
    print("\n--- Step 2.5: Refine Your Job Search List ---")
    print("The AI has suggested the following job titles:")
    job_titles = initial_titles[:]  # Work with a copy

    while True:
        for i, title in enumerate(job_titles, 1):
            print(f"  [{i}] {title}")

        prompt = (
            "\nEnter numbers to keep, and/or type your own titles to add."
            "\n(e.g., 1, 4, Senior Data Analyst, Prompt Engineer)."
            "\nPress ENTER to search for all AI suggestions.\n> "
        )
        user_input = input(prompt).strip()

        if not user_input:
            logger.info("User accepted all AI-suggested job titles.")
            return job_titles

        final_titles, has_error = [], False
        items = [item.strip() for item in user_input.split(',') if item.strip()]

        for item in items:
            try:
                num = int(item)
                if 1 <= num <= len(job_titles):
                    final_titles.append(job_titles[num - 1])
                else:
                    print(f"\nError: Number '{num}' is out of range (1-{len(job_titles)}).")
                    has_error = True
                    break
            except ValueError:
                final_titles.append(item)

        if has_error:
            continue

        # De-duplicate the list while preserving order
        unique_titles = list(dict.fromkeys(final_titles))
        if unique_titles:
            logger.info(f"User refined the search list to: {unique_titles}")
            return unique_titles

        print("\nError: Your selection resulted in an empty list. Please try again.\n")


def launch_scrapers(job_titles: List[str], settings: Dict[str, Any]) -> List[Dict]:
    """
    Launches all scraping tasks in parallel using a ThreadPoolExecutor.
    """
    logger.info("--- Step 3: Launching All Scrapers in Parallel ---")
    all_scraped_jobs = []
    with ThreadPoolExecutor(max_workers=12) as executor:
        future_to_scraper = {}
        for title in job_titles:
            future_to_scraper[executor.submit(
                run_scraper_task, JobSpyScraper, keyword=title,
                location=settings['location'],
                results_wanted=settings['results_per_site'],
                country_indeed=settings['country']
            )] = "JobSpy"
            future_to_scraper[executor.submit(
                run_scraper_task, UnstopScraper, keyword=title,
                max_pages=settings['pages_for_others']
            )] = "Unstop"

        future_to_scraper[executor.submit(
            run_scraper_task, InstahyreScraper, job_function_id=9,
            max_pages=settings['pages_for_others']
        )] = "Instahyre"

        logger.info(f"Submitted {len(future_to_scraper)} scraping tasks.")

        for future in as_completed(future_to_scraper):
            scraper_name = future_to_scraper[future]
            try:
                if results := future.result():
                    all_scraped_jobs.extend(results)
            # Justification: The inner task runner already logs specifics. This is a
            # final safeguard for any error from the future itself.
            except Exception as e: # pylint: disable=broad-exception-caught
                logger.error(f"Task for {scraper_name} generated an exception: {e}")

    logger.success(
        "All scrapers finished. Total jobs collected (before de-duplication): "
        f"{len(all_scraped_jobs)}"
    )
    return all_scraped_jobs


def main():
    """
    Main execution function that orchestrates the entire job search process.
    """
    logger.info("--- Step 1: Gathering User Input ---")
    settings = get_user_settings()

    initial_titles = get_ai_job_titles(settings)

    final_job_titles = refine_job_titles_interactively(initial_titles)
    logger.success(f"Final job titles for scraping: {final_job_titles}")

    all_jobs = launch_scrapers(final_job_titles, settings)

    if all_jobs:
        logger.info("--- Step 4: Processing and Saving Data ---")
        processor = DataProcessor(output_dir="scraped_data")
        output_file = processor.save_to_excel(all_jobs)
        if output_file:
            logger.info(f"Process complete. Final report saved to: {output_file}")
        else:
            logger.error("Data processing failed. No output file was generated.")
    else:
        logger.warning("No jobs were collected. Skipping data processing.")


if __name__ == "__main__":
    setup_logging()
    try:
        main()
    # Justification: This is the final, top-level safeguard for the entire
    # application. Catching 'Exception' here is appropriate to log any unhandled
    # error and ensure a clean exit message.
    except Exception: 
        logger.exception("An unhandled exception occurred in the main orchestrator.")
    finally:
        logger.info("--- Job Search Session Finished ---\n")