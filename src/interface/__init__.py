"""
Interactive interface module for the Text-to-SQL Agent.
Provides terminal-based user interface with rich features.
"""

from .terminal import InteractiveAgent
from .commands import CommandHandler

__all__ = [
    'InteractiveAgent',
    'CommandHandler',
    'run_interactive_session'
]

__version__ = '1.0.0'


def run_interactive_session(config: dict = None):
    """
    Run an interactive Text-to-SQL session.
    
    This is a convenience function to quickly start an interactive session
    with optional configuration.
    
    Args:
        config: Optional configuration dictionary with keys:
            - api_key: OpenAI API key
            - db_path: Path to database file
            - model: OpenAI model to use
            - colors_enabled: Enable colored output
            - verbose_mode: Enable verbose output
            - auto_export: Auto-export results
            - enable_optimization: Enable query optimization
            - enable_validation: Enable query validation
    
    Example:
        >>> from src.interface import run_interactive_session
        >>> run_interactive_session({
        ...     'api_key': 'your-api-key',
        ...     'db_path': 'data/my_database.db',
        ...     'model': 'gpt-4'
        ... })
    """
    try:
        # Create and run the interactive agent
        agent = InteractiveAgent(config)
        agent.run()
        
    except KeyboardInterrupt:
        print("\n\nSession interrupted by user.")
        
    except Exception as e:
        print(f"Error running interactive session: {e}")
        import traceback
        traceback.print_exc()


# Color scheme configuration for the interface
COLOR_SCHEMES = {
    'default': {
        'prompt': 'GREEN',
        'info': 'BLUE',
        'success': 'GREEN',
        'warning': 'YELLOW',
        'error': 'RED',
        'header': 'CYAN',
        'sql': 'MAGENTA'
    },
    'dark': {
        'prompt': 'CYAN',
        'info': 'WHITE',
        'success': 'GREEN',
        'warning': 'YELLOW',
        'error': 'RED',
        'header': 'BLUE',
        'sql': 'YELLOW'
    },
    'light': {
        'prompt': 'BLUE',
        'info': 'BLACK',
        'success': 'GREEN',
        'warning': 'YELLOW',
        'error': 'RED',
        'header': 'MAGENTA',
        'sql': 'BLUE'
    }
}

# Default configuration template
DEFAULT_CONFIG = {
    'api_key': None,
    'db_path': 'data/ecommerce.db',
    'model': 'gpt-4',
    'colors_enabled': True,
    'verbose_mode': False,
    'auto_export': False,
    'history_file': '.agent_history',
    'max_history': 100,
    'enable_optimization': True,
    'enable_validation': True,
    'color_scheme': 'default',
    'export_format': 'csv',
    'max_display_rows': 100,
    'cache_enabled': True,
    'cache_ttl': 3600,
    'debug_mode': False
}

# Command shortcuts for quick access
COMMAND_SHORTCUTS = {
    'h': 'help',
    'q': 'quit',
    'c': 'clear',
    's': 'stats',
    'e': 'export',
    'v': 'verbose',
    'o': 'optimize',
    '?': 'help',
    '!': 'shell',
    '@': 'schema',
    '#': 'tables'
}

# Example queries organized by difficulty
EXAMPLE_QUERIES = {
    'beginner': [
        "Show all customers",
        "List products under $50",
        "Find orders from today",
        "Count total products",
        "Show product categories"
    ],
    'intermediate': [
        "What are the top 5 best-selling products?",
        "Show customers who made more than 3 orders",
        "Calculate total revenue by month",
        "Find products that are low in stock",
        "List orders with total over $500"
    ],
    'advanced': [
        "Which products are frequently bought together?",
        "Calculate customer lifetime value for VIP customers",
        "Show month-over-month revenue growth",
        "Find customers who haven't ordered in 60 days",
        "Analyze product performance by category and brand"
    ],
    'expert': [
        "Identify potential fraud patterns in orders",
        "Calculate inventory turnover rate by category",
        "Segment customers by purchase behavior",
        "Forecast next month's revenue based on trends",
        "Find cross-selling opportunities between categories"
    ]
}


class InterfaceConfig:
    """
    Configuration manager for the interface module.
    """
    
    def __init__(self, config_dict: dict = None):
        """
        Initialize configuration with defaults and overrides.
        
        Args:
            config_dict: Optional configuration overrides
        """
        self.config = DEFAULT_CONFIG.copy()
        if config_dict:
            self.config.update(config_dict)
        
        # Load from environment if not provided
        self._load_from_environment()
        
        # Validate configuration
        self._validate()
    
    def _load_from_environment(self):
        """Load configuration from environment variables."""
        import os
        from dotenv import load_dotenv
        
        load_dotenv()
        
        env_mappings = {
            'OPENAI_API_KEY': 'api_key',
            'DATABASE_PATH': 'db_path',
            'MODEL_NAME': 'model',
            'COLORS_ENABLED': ('colors_enabled', lambda x: x.lower() == 'true'),
            'VERBOSE_MODE': ('verbose_mode', lambda x: x.lower() == 'true'),
            'DEBUG_MODE': ('debug_mode', lambda x: x.lower() == 'true'),
            'CACHE_ENABLED': ('cache_enabled', lambda x: x.lower() == 'true'),
            'CACHE_TTL': ('cache_ttl', int),
            'MAX_HISTORY': ('max_history', int),
            'MAX_DISPLAY_ROWS': ('max_display_rows', int)
        }
        
        for env_key, mapping in env_mappings.items():
            env_value = os.getenv(env_key)
            if env_value is not None:
                if isinstance(mapping, tuple):
                    config_key, converter = mapping
                    self.config[config_key] = converter(env_value)
                else:
                    self.config[mapping] = env_value
    
    def _validate(self):
        """Validate configuration values."""
        # Check required fields
        if not self.config.get('db_path'):
            raise ValueError("Database path is required")
        
        # Validate model
        valid_models = ['gpt-4', 'gpt-3.5-turbo', 'gpt-4-turbo']
        if self.config['model'] not in valid_models:
            raise ValueError(f"Invalid model. Must be one of: {valid_models}")
        
        # Validate numeric values
        if self.config['max_history'] < 1:
            self.config['max_history'] = 1
        
        if self.config['max_display_rows'] < 1:
            self.config['max_display_rows'] = 1
        
        if self.config['cache_ttl'] < 0:
            self.config['cache_ttl'] = 0
    
    def get(self, key: str, default=None):
        """
        Get configuration value.
        
        Args:
            key: Configuration key
            default: Default value if key not found
            
        Returns:
            Configuration value
        """
        return self.config.get(key, default)
    
    def set(self, key: str, value):
        """
        Set configuration value.
        
        Args:
            key: Configuration key
            value: Configuration value
        """
        self.config[key] = value
        self._validate()
    
    def to_dict(self) -> dict:
        """
        Get configuration as dictionary.
        
        Returns:
            Configuration dictionary
        """
        return self.config.copy()


# Initialize module-level logger
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)