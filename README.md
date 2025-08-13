# JobsAgent - AI Job Scraper

A simple Python tool that uses AI to find relevant jobs for you automatically.

## What it does

JobsAgent reads your resume and career interests, then:
1. Uses Google's Gemini AI to suggest job titles that match your profile
2. Searches multiple job websites for those positions
3. Collects all the job listings into one Excel file

## How it works

The tool has three main parts:

**Job Finder (`core/job_finder.py`)**
- Sends your resume to Google Gemini AI
- Gets back a list of job titles that fit your background

**Scrapers (`scrapers/` folder)**
- `instahyre_scraper.py` - Gets jobs from Instahyre
- `jobspy_scraper.py` - Gets jobs from multiple sites using JobSpy library
- `unstop_scraper.py` - Gets jobs from Unstop

**Data Processor (`core/data_processor.py`)**
- Removes duplicate job listings
- Cleans up the data
- Saves everything to an Excel file

## Setup

1. **Install Python** (version 3.8 or higher)

2. **Download the project**
   ```bash
   git clone https://github.com/anuvratgautam/JobsAgent.git
   cd JobsAgent
   ```

3. **Install required packages**
   ```bash
   pip install -r requirements.txt
   ```

4. **Get a Google Gemini API key**
   - Go to Google AI Studio
   - Create an API key
   - Put it in `config.py`:
     ```python
     GEMINI_API_KEY = "your-api-key-here"
     ```

## Usage

1. **Run the program**
   ```bash
   python main.py
   ```

2. **Follow the prompts**
   - Enter the path to your resume file
   - Type in your career interests (e.g., "software development, data analysis")

3. **Wait for results**
   - The program will search job sites automatically
   - Results will be saved as an Excel file with timestamp

## Example Output

The Excel file will contain columns like:
- Job Title
- Company Name
- Location
- Salary Range
- Job Description
- Application Link

## Project Structure

```
JobsAgent/
├── main.py              # Main program file
├── config.py            # Settings and API keys
├── requirements.txt     # Required Python packages
├── core/
│   ├── job_finder.py    # AI job title generator
│   └── data_processor.py # Data cleaning
└── scrapers/
    ├── instahyre_scraper.py
    ├── jobspy_scraper.py
    └── unstop_scraper.py
```

## Notes

- Make sure you have a stable internet connection
- The program may take a few minutes to run depending on how many jobs it finds
- Check the generated Excel file for your job listings
- Logs are created to track what the program is doing
