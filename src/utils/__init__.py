"""
Utility modules for the Text-to-SQL Agent.
Provides formatting, caching, and helper functions.
"""

from .formatter import (
    ResultFormatter,
    QueryFormatter,
    ProgressFormatter,
    result_formatter,
    query_formatter,
    progress_formatter
)

from .cache import (
    QueryCache,
    ResultCache,
    CacheManager,
    cache_manager
)

__all__ = [
    # Formatter classes
    'ResultFormatter',
    'QueryFormatter',
    'ProgressFormatter',
    
    # Formatter instances
    'result_formatter',
    'query_formatter',
    'progress_formatter',
    
    # Cache classes
    'QueryCache',
    'ResultCache',
    'CacheManager',
    
    # Cache instance
    'cache_manager'
]

__version__ = '1.0.0'