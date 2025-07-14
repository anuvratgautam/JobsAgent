# config.py

import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Get the API key securely
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")

if not GOOGLE_API_KEY:
    raise ValueError("GOOGLE_API_KEY is not set. Please set it in your .env file.")
