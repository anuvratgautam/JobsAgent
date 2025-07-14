# scrapers/__init__.py

"""
This file makes the 'scrapers' directory a Python package and acts as a
central registry for all available scraper classes.

By importing the main scraper class from each module here, we can simplify
imports in the main application orchestrator (main.py). This follows
best practices for modular and maintainable code.
"""

# --- Primary Multi-Site Scraper ---
# This is the main scraper that handles Indeed, LinkedIn, Google, and Naukri.
from .jobspy_scraper import JobSpyScraper

# --- Specialized Custom Scrapers ---
# These are the scrapers you've built for sites not covered by JobSpy.
from .unstop_scraper import UnstopScraper
from .instahyre_scraper import InstahyreScraper


# To add a new custom scraper in the future (e.g., for 'wellfound.com'):
# 1. Create the new scraper file: 'scrapers/wellfound_scraper.py'
# 2. Implement the 'WellfoundScraper' class inside it, following the blueprint.
# 3. Add the import line here:
#    from .wellfound_scraper import WellfoundScraper