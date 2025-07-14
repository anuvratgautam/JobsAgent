# core/job_finder.py

import google.generativeai as genai
from loguru import logger
from typing import List, Optional
from pathlib import Path

# We need PyPDF2 to read resume content from PDF files
try:
    import PyPDF2
except ImportError:
    print("Error: PyPDF2 is not installed. Please run 'pip install PyPDF2'")
    PyPDF2 = None

class JobFinder:
    """
    Uses the Google Gemini AI to analyze a user's resume and interests
    and suggest a list of relevant job titles to search for.
    
    Can read resumes from both .txt and .pdf files.
    """

    # --- 1. Configuration ---
    _AI_MODEL_NAME = "gemini-2.0-flash"
    _SYSTEM_PROMPT = """
You are an expert career advisor and an AI assistant for a job scraping tool.
Your sole purpose is to analyze the provided resume text and user interests,
and then generate a concise, clean, comma-separated list of relevant job titles to search for.

**Rules:**
1.  **Analyze Holistically:** Consider the skills, experience, and projects in the resume.
2.  **Incorporate Interests:** Give weight to the user's stated interests.
3.  **Generate Diverse Titles:** Suggest a variety of titles, from standard to more specific ones (e.g., "Data Scientist", "ML Engineer", "NLP Specialist").
4.  **Output Format is CRITICAL:** Return ONLY a single line of text containing job titles separated by commas.
5.  **DO NOT** add any introductory phrases like "Here is a list..." or "Based on your resume...".
6.  **DO NOT** use markdown, numbering, or bullet points.
7.  **Example Output:** Machine Learning Engineer, Data Analyst, AI Specialist, Python Developer, Research Scientist
"""

    # --- 2. Initialization ---
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
            self.log.info(f"AI client configured successfully for model: '{self._AI_MODEL_NAME}'.")
        except Exception as e:
            self.log.critical(f"Failed to configure Google AI client. Error: {e}")
            raise

    # --- 3. Private Helper Methods (The "How") ---
    def _read_resume_content(self, resume_path: str) -> Optional[str]:
        """
        Reads the text content from a given resume file path.
        Supports both .txt and .pdf files.

        Args:
            resume_path (str): The full path to the resume file.

        Returns:
            Optional[str]: The text content of the file, or None if an error occurs.
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
                    self.log.error("PyPDF2 library is required to read PDF files but is not installed.")
                    return None
                with open(path, 'rb') as file:
                    reader = PyPDF2.PdfReader(file)
                    for page in reader.pages:
                        content += page.extract_text() or ""
            elif file_extension == '.txt':
                with open(path, 'r', encoding='utf-8') as f:
                    content = f.read()
            else:
                self.log.error(f"Unsupported resume file format: '{file_extension}'. Please use .txt or .pdf.")
                return None
            
            self.log.success(f"Successfully extracted text from '{path.name}'.")
            return content
        except Exception as e:
            self.log.error(f"An error occurred while reading the resume file '{path.name}'. Error: {e}")
            return None

    # --- 4. Public Method (The "What") ---
    def get_job_titles(self, resume_path: str, user_interests: str) -> List[str]:
        self.log.info("Attempting to generate job titles from resume and interests...")

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

            job_titles = [title.strip() for title in raw_titles.split(',') if title.strip()]

            if not job_titles:
                self.log.warning("AI returned a response, but it was empty or in an invalid format.")
                return []

            return job_titles
        except Exception as e:
            self.log.error(f"An error occurred while calling the Gemini API. Error: {e}")
            return []