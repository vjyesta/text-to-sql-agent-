"""
Database package public API.

Exposes database creation, seeding utilities, and schema helpers for
easy imports across the project.
"""

from .creator import DatabaseCreator
from .seeder import DatabaseSeeder
from .schema import extract_schema_info, generate_schema_description

__all__ = [
    'DatabaseCreator',
    'DatabaseSeeder',
    'extract_schema_info',
    'generate_schema_description',
]

__version__ = '1.0.0'


