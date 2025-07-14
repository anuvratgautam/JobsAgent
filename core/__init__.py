# core/__init__.py

"""
Core Package Initializer

This file makes the main classes from the core modules directly importable
from the `core` package, simplifying access for other parts of the application
like main.py.

It also defines the public API of the package using __all__.
"""

# Import the main classes from their respective modules
from .data_processor import DataProcessor
from .job_finder import JobFinder

# Define the public API of the 'core' package.
# When a user writes 'from core import *', only these names will be imported.
# This is a best practice for clean package design.
__all__ = [
    "DataProcessor",
    "JobFinder",
]