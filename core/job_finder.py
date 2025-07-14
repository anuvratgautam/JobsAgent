# core/job_finder.py

"""
AI-powered job title suggestion module.

This module provides the JobFinder class, which uses the Google Gemini AI
to analyze a user's resume and stated interests. It then generates a list
of relevant job titles to be used as search keywords by the scrapers.
It supports reading resume content from both .txt and .pdf files.
"""
from pathlib import Path
from typing import List, Optional

import google.generativeai as genai
from loguru import logger

# PyPDF2 is an optional dependency for reading PDF resumes.
try:
    import PyPDF2
except ImportError:
    print("Warning: PyPDF2 is not installed. PDF resume parsing will not be available.")
    print("Please run 'pip install PyPDF2' to enable this feature.")
    PyPDF2 = None


# pylint: disable=too-few-public-methods
class JobFinder:
    """
    Uses Google Gemini AI to suggest job titles from a resume and user interests.
    """
    _AI_MODEL_NAME = "gemini-1.5-flash"
    _SYSTEM_PROMPT = """
You are an expert career advisor and an AI assistant for a job scraping tool.
Your sole purpose is to analyze the provided resume text and user interests,
and then generate a concise, clean, comma-separated list of relevant job titles.

**Rules:**
1.  Analyze the skills, experience, and projects in the resume.
2.  Give weight to the user's stated interests.
3.  Suggest a variety of titles (e.g., "Data Scientist", "ML Engineer").
4.  **Output Format is CRITICAL:** Return ONLY a single line of text
    containing job titles separated by commas.
5.  **DO NOT** add any introductory phrases like "Here is a list...".
6.  **DO NOT** use markdown, numbering, or bullet points.
7.  **Example Output:** Machine Learning Engineer, Data Analyst, AI Specialist
"""

    def __init__(self, api_key: str):
        if not api_key:
            raise ValueError("Google API key is required to initialize JobFinder.")

        self.log = logger.bind(source="JobFinder")
        try:
            genai.configure(api_key=api_key)
            self.model = genai.GenerativeModel(
                model_name=self._AI_MODEL_NAME,
                system_instruction=self._SYSTEM_PROMPT
            )
            self.log.info(
                f"AI client configured successfully for model: '{self._AI_MODEL_NAME}'."
            )
        # Justification: A failure to configure the AI client is a critical,
        # show-stopping error. Raising the original exception is appropriate.
        except Exception as e:  # pylint: disable=broad-exception-caught
            self.log.critical(f"Failed to configure Google AI client. Error: {e}")
            raise

    def _read_resume_content(self, resume_path: str) -> Optional[str]:
        """
        Reads the text content from a given .txt or .pdf resume file.

        Args:
            resume_path: The full path to the resume file.

        Returns:
            The text content of the file, or None if an error occurs.
        """
        path = Path(resume_path)
        content = ""

        if not path.is_file():
            self.log.error(f"Resume file not found at path: {resume_path}")
            return None

        file_extension = path.suffix.lower()
        self.log.info(f"Attempting to read resume with extension: '{file_extension}'")

        try:
            if file_extension == '.pdf':
                if not PyPDF2:
                    self.log.error("PyPDF2 is not installed. Cannot read PDF file.")
                    return None
                with open(path, 'rb') as file:
                    reader = PyPDF2.PdfReader(file)
                    content = "".join(page.extract_text() or "" for page in reader.pages)
            elif file_extension == '.txt':
                content = path.read_text(encoding='utf-8')
            else:
                self.log.error(f"Unsupported resume format: '{file_extension}'.")
                return None

            self.log.success(f"Successfully extracted text from '{path.name}'.")
            return content
        # Justification: File I/O can fail for many reasons (permissions, corrupt
        # files, etc.). We want to log any such error and return None gracefully.
        except Exception as e:  # pylint: disable=broad-exception-caught
            self.log.error(f"Error reading resume file '{path.name}'. Error: {e}")
            return None

    def get_job_titles(self, resume_path: str, user_interests: str) -> List[str]:
        """
        Generates a list of job titles by calling the Gemini API.

        Args:
            resume_path: The full path to the user's resume file.
            user_interests: A string of the user's stated job interests.

        Returns:
            A list of suggested job titles, or an empty list on failure.
        """
        self.log.info("Generating job titles from resume and interests...")

        resume_content = self._read_resume_content(resume_path)
        if not resume_content:
            return []

        user_prompt = (
            f"**Resume Text:**\n{resume_content}\n\n"
            f"**User's Stated Interests:**\n{user_interests}"
        )

        try:
            self.log.debug("Sending request to Gemini API...")
            response = self.model.generate_content(user_prompt)

            raw_titles = response.text.strip()
            self.log.success(f"Received raw response from AI: '{raw_titles}'")

            job_titles = [
                title.strip() for title in raw_titles.split(',') if title.strip()
            ]

            if not job_titles:
                self.log.warning("AI response was empty or in an invalid format.")
                return []

            return job_titles
        # Justification: Any error from the external Gemini API should be caught,
        # logged, and result in an empty list to prevent the app from crashing.
        except Exception as e:  # pylint: disable=broad-exception-caught
            self.log.error(f"An error occurred calling the Gemini API. Error: {e}")
            return []